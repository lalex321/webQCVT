from __future__ import annotations

import copy
import inspect
import json
import os
import re
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional


class LowRelevanceError(Exception):
    """Raised when CV is not relevant to the provided JD."""
    pass

_JD_MARKERS = {'experience', 'requirements', 'responsibilities', 'qualifications', 'role',
               'skills', 'position', 'candidate', 'looking for', 'must have', 'nice to have',
               'employment', 'salary', 'location', 'remote', 'hybrid', 'full-time', 'part-time',
               'about the role', 'job description', 'what you', 'who you', 'we are looking',
               'you will', 'you should', 'your role', 'the team', 'reporting to', 'hiring'}

def validate_jd(jd_text: str) -> str | None:
    """Validate JD text. Returns error message or None if valid."""
    if not jd_text or not jd_text.strip():
        return "Job Description is empty."
    text = jd_text.strip()
    if len(text) < 20:
        return f"Job Description is too short ({len(text)} chars). Please provide a full JD."
    words = text.split()
    if len(words) < 5:
        return f"Job Description has only {len(words)} words. Please provide a more detailed JD."
    return None

from google import genai
from google.genai import types as genai_types

_gemini_api_key: str = ""
from docx import Document
from pypdf import PdfReader

import cv_engine as core

StatusCallback = Callable[[str, int], None]


@dataclass
class JobState:
    job_id: str
    filename: str
    status: str = "Queued"
    progress: int = 0
    error: Optional[str] = None
    result_path: Optional[str] = None
    anonymize: bool = False
    autofix: bool = False
    template_name: str = "quantori_classic.docx"
    debug: str = ""
    created_at: float = field(default_factory=time.time)


class InMemoryJobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, JobState] = {}
        self._lock = threading.Lock()

    def create(
        self,
        filename: str,
        anonymize: bool = False,
        autofix: bool = False,
        template_name: str = "quantori_classic.docx",
    ) -> JobState:
        job = JobState(
            job_id=str(uuid.uuid4()),
            filename=filename,
            anonymize=anonymize,
            autofix=autofix,
            template_name=template_name,
        )
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def get(self, job_id: str) -> Optional[JobState]:
        with self._lock:
            return self._jobs.get(job_id)

    def update(
        self,
        job_id: str,
        *,
        status: Optional[str] = None,
        progress: Optional[int] = None,
        error: Optional[str] = None,
        result_path: Optional[str] = None,
        debug: Optional[str] = None,
    ) -> None:
        with self._lock:
            job = self._jobs[job_id]
            if status is not None:
                job.status = status
            if progress is not None:
                job.progress = progress
            if error is not None:
                job.error = error
            if result_path is not None:
                job.result_path = result_path
            if debug is not None:
                job.debug = debug

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for j in self._jobs.values() if j.status not in ("Done", "Failed", "Low Relevance", "Queued"))

    def cleanup_old(self, max_age_sec: int = 3600) -> list[str]:
        """Remove finished jobs older than max_age_sec. Returns list of removed job IDs."""
        cutoff = time.time() - max_age_sec
        removed = []
        with self._lock:
            for jid, job in list(self._jobs.items()):
                if job.status in ("Done", "Failed", "Low Relevance") and job.created_at < cutoff:
                    removed.append(jid)
            for jid in removed:
                del self._jobs[jid]
        return removed



def _slug_part(value: str) -> str:
    text = re.sub(r"[^A-Za-zА-Яа-я0-9_-]+", "_", str(value or "").strip())
    text = re.sub(r"_+", "_", text).strip("_")
    return text


_degree_suffixes_re = re.compile(r',?\s*\b(PhD|Ph\.?D\.?|MD|M\.D\.?|DSc|D\.Sc\.?|Dr\.?)\b\.?', re.IGNORECASE)

def _build_output_base_name(data: dict, anonymize: bool, tailor: bool = False, fallback: str = "Converted") -> str:
    basics = data.get("basics") or {}
    raw_name = str((basics.get("name") or data.get("name") or "")).strip()
    # Strip degree suffixes before building filename
    raw_name = _degree_suffixes_re.sub('', raw_name).strip()
    parts = [p for p in re.split(r"\s+", raw_name) if p]

    suffix = "_tailored_a" if tailor and anonymize else "_tailored" if tailor else ""

    if len(parts) >= 2:
        first = _slug_part(parts[0])
        last = _slug_part(parts[-1])
        if anonymize:
            if first and last:
                return f"CV_{first}_{last[:1]}{suffix}"
            if first:
                return f"CV_{first}{suffix}"
        else:
            if first and last:
                return f"CV_{first}_{last}{suffix}"
            if first:
                return f"CV_{first}{suffix}"

    if len(parts) == 1:
        first = _slug_part(parts[0])
        if first:
            return f"CV_{first}{suffix}"

    fb = _slug_part(fallback) or "Converted"
    return f"CV_{fb}"


def _repair_json(s: str) -> str:
    """Best-effort repair of common LLM JSON errors."""
    # Remove control characters except \n \r \t
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', s)
    # Fix single quotes → double quotes (outside of already double-quoted strings)
    s = re.sub(r"(?<![\"\\])'([^']*)'(?!\")", r'"\1"', s)
    # Fix trailing commas before } or ]
    s = re.sub(r',\s*([}\]])', r'\1', s)
    # Fix missing commas between } and { or between "value" and "key"
    s = re.sub(r'(\})\s*(\{)', r'\1,\2', s)
    s = re.sub(r'(")\s*\n\s*(")', r'\1,\n\2', s)
    # Fix unquoted keys: word followed by colon
    s = re.sub(r'(?<=[\{,\n])\s*([a-zA-Z_]\w*)\s*:', r' "\1":', s)
    return s

def extract_first_json_object(text: str):
    if text is None:
        raise ValueError("No text to parse")
    s = str(text).strip()
    if not s:
        raise ValueError("Empty text")

    fence = re.search(r"```(?:json)?\s*(.*?)```", s, re.S | re.I)
    if fence:
        s = fence.group(1).strip()

    try:
        return json.loads(s)
    except Exception:
        pass

    starts = [i for i in [s.find("{"), s.find("[")] if i != -1]
    if not starts:
        raise ValueError("No JSON object or array found")

    start = min(starts)
    decoder = json.JSONDecoder()
    try:
        obj, _ = decoder.raw_decode(s[start:])
        return obj
    except json.JSONDecodeError:
        pass

    # Attempt repair and retry
    repaired = _repair_json(s[start:])
    try:
        return json.loads(repaired)
    except json.JSONDecodeError:
        pass

    # Last resort: try raw_decode on repaired text
    try:
        obj, _ = decoder.raw_decode(repaired)
        return obj
    except json.JSONDecodeError:
        pass

    # Final fallback: json_repair library
    try:
        from json_repair import repair_json
        fixed = repair_json(s[start:], return_objects=True)
        if isinstance(fixed, (dict, list)):
            return fixed
    except Exception:
        pass

    raise ValueError("Could not parse JSON from LLM response")


def read_source_text(source_path: Path | str) -> str:
    source_path = Path(source_path)
    suffix = source_path.suffix.lower()

    if suffix == ".pdf":
        reader = PdfReader(str(source_path))
        pages = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        return "\n\n".join(pages).strip()

    if suffix == ".docx":
        try:
            from source_baseline_extractor import extract_from_docx as _extract_from_docx
            from cv_engine import _format_docx_sections_for_llm
            baseline = _extract_from_docx(str(source_path))
            text = _format_docx_sections_for_llm(baseline)
            if text:
                return text
        except Exception:
            pass
        doc = Document(str(source_path))
        parts = []
        for p in doc.paragraphs:
            txt = (p.text or "").strip()
            if txt:
                parts.append(txt)
        return "\n".join(parts).strip()

    raise ValueError(f"Unsupported source file: {source_path.name}")


def _is_supported_image_file(source_path: Path | str) -> bool:
    source_path = Path(source_path)
    return source_path.suffix.lower() in {".png", ".jpg", ".jpeg"}


def _mime_type_for_source(source_path: Path | str) -> str:
    source_path = Path(source_path)
    suffix = source_path.suffix.lower()
    if suffix == ".png":
        return "image/png"
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    raise ValueError(f"Unsupported image file: {source_path.name}")


def _retry_on_rate_limit(fn, max_retries=5):
    """Retry a callable on 429 / RESOURCE_EXHAUSTED with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            err = str(e)
            if ("429" in err or "Resource" in err or "Quota" in err) and attempt < max_retries - 1:
                delay = [5, 5, 5, 10, 10][attempt]
                print(f"⚠️ API rate limit. Sleeping {delay}s (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                raise


def call_llm_json_for_uploaded_file(prompt: str, model_name: str, source_path: Path | str):
    source_path = Path(source_path)
    client = genai.Client(api_key=_gemini_api_key)
    mime_type = _mime_type_for_source(source_path)
    uploaded = client.files.upload(
        file=str(source_path),
        config=genai_types.UploadFileConfig(mime_type=mime_type),
    )

    while getattr(uploaded, "state", None) and getattr(uploaded.state, "name", "") == "PROCESSING":
        time.sleep(1)
        uploaded = client.files.get(name=uploaded.name)

    state_name = getattr(getattr(uploaded, "state", None), "name", "")
    if state_name and state_name != "ACTIVE":
        raise RuntimeError(f"Uploaded file is not ready: {state_name}")

    response = _retry_on_rate_limit(
        lambda: client.models.generate_content(model=model_name, contents=[uploaded, prompt])
    )
    txt = getattr(response, "text", None)
    if not txt:
        raise RuntimeError("Model returned empty response")
    return extract_first_json_object(txt)




# Common stop words for JD/CV keyword comparison (used by both relevance check and keyword report)
_JD_STOP_WORDS = {
    'the', 'and', 'for', 'with', 'from', 'that', 'this', 'are', 'was', 'were',
    'has', 'have', 'been', 'will', 'can', 'not', 'but', 'also', 'such', 'other',
    'all', 'any', 'each', 'our', 'you', 'your', 'their', 'them', 'who', 'what',
    'when', 'how', 'more', 'most', 'some', 'than', 'into', 'over', 'about',
    'experience', 'years', 'role', 'work', 'working', 'strong', 'ability',
    'team', 'teams', 'including', 'across', 'within', 'using', 'based',
    'senior', 'junior', 'lead', 'manager', 'head', 'director', 'staff',
    'full', 'high', 'time', 'level', 'new', 'key', 'well', 'good', 'best',
    'systems', 'system', 'management', 'client', 'clients', 'project',
    'projects', 'delivery', 'process', 'service', 'services', 'support',
    'requirements', 'quality', 'development', 'performance', 'business',
    'communication', 'skills', 'knowledge', 'solutions', 'environment',
}


def _as_clean_list(value):
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, str):
                s = item.strip()
                if s:
                    out.append(s)
            elif isinstance(item, dict):
                out.append(item)
        return out
    return []


def _count_summary_bullets(data: dict) -> int:
    summary = data.get("summary") or {}
    if not isinstance(summary, dict):
        return 0
    bullets = summary.get("bullet_points") or summary.get("items") or []
    if not isinstance(bullets, list):
        return 0
    return len([x for x in bullets if isinstance(x, str) and x.strip()])


def _count_skill_groups(data: dict) -> int:
    skills = data.get("skills") or {}
    if not isinstance(skills, dict):
        return 0
    count = 0
    for value in skills.values():
        if isinstance(value, list) and any(str(x).strip() for x in value if x is not None):
            count += 1
        elif isinstance(value, str) and value.strip():
            count += 1
    return count


def _is_nonempty_education_entry(entry) -> bool:
    if not isinstance(entry, dict):
        return bool(str(entry).strip())
    for key in ("degree", "institution", "school", "field", "dates", "year"):
        if str(entry.get(key) or "").strip():
            return True
    return False


def _compute_jd_keyword_report(data: dict, jd_text: str) -> dict:
    """Compare JD keywords against tailored CV to produce matched/missing/added report."""
    def _extract_words(text: str) -> set[str]:
        return set(w.lower() for w in re.findall(r'[A-Za-z#+.]{3,}', text)) - _JD_STOP_WORDS

    # JD keywords
    jd_words = _extract_words(jd_text)

    # CV keywords — broad scan across all text-bearing fields
    cv_text_parts: list[str] = []
    cv_text_parts.append(str(data.get("basics", {}).get("current_title", "")))
    for bullet in (data.get("summary") or []):
        cv_text_parts.append(str(bullet))
    for cat, items in (data.get("skills") or {}).items():
        cv_text_parts.append(cat)
        if isinstance(items, list):
            cv_text_parts.extend(str(s) for s in items)
    for exp in (data.get("experience") or []):
        cv_text_parts.append(str(exp.get("role", "")))
        cv_text_parts.append(str(exp.get("company", "")))
        for acc in (exp.get("accomplishments") or []):
            cv_text_parts.append(str(acc))
        for env_item in (exp.get("environment") or []):
            cv_text_parts.append(str(env_item))
    for edu in (data.get("education") or []):
        if isinstance(edu, dict):
            cv_text_parts.append(str(edu.get("degree", "")))
            cv_text_parts.append(str(edu.get("field", "")))
    for cert in (data.get("certifications") or []):
        cv_text_parts.append(str(cert))

    cv_words = set()
    for part in cv_text_parts:
        cv_words |= _extract_words(part)

    matched = sorted(jd_words & cv_words)
    missing = sorted(jd_words - cv_words)
    added = sorted(cv_words - jd_words)

    jd_count = len(jd_words)
    match_pct = round(len(matched) / jd_count * 100) if jd_count else 0

    return {
        "matched": matched,
        "missing": missing,
        "added": added,
        "jd_keyword_count": jd_count,
        "cv_keyword_count": len(cv_words),
        "match_pct": match_pct,
    }


def _build_content_details(data: dict, *, template_name: str, anonymize: bool, source_path: Path, jd_text: str = "") -> dict:
    basics = data.get("basics") or {}
    summary_count = _count_summary_bullets(data)
    skill_group_count = _count_skill_groups(data)
    experience_entries = len([x for x in _as_clean_list(data.get("experience")) if isinstance(x, dict) or str(x).strip()])
    project_entries = len([x for x in _as_clean_list(data.get("projects")) if isinstance(x, dict) or str(x).strip()])
    education_entries = len([x for x in _as_clean_list(data.get("education")) if _is_nonempty_education_entry(x)])
    certification_entries = len([x for x in _as_clean_list(data.get("certifications")) if isinstance(x, str) and x.strip()])
    language_entries = len([x for x in _as_clean_list(data.get("languages")) if isinstance(x, dict) or str(x).strip()])

    other_sections = []
    for sec in _as_clean_list(data.get("other_sections")):
        if not isinstance(sec, dict):
            continue
        title = str(sec.get("title") or "").strip()
        items = sec.get("items") or []
        has_items = isinstance(items, list) and any(str(x).strip() for x in items if x is not None)
        if title and has_items:
            other_sections.append(title)

    rendered = []
    omitted = []

    def add_section(name: str, present: bool) -> None:
        (rendered if present else omitted).append(name)

    add_section("summary", summary_count > 0)
    add_section("skills", skill_group_count > 0)
    add_section("experience", experience_entries > 0)
    add_section("projects", project_entries > 0)
    add_section("education", education_entries > 0)
    add_section("certifications", certification_entries > 0)
    add_section("languages", language_entries > 0)
    if other_sections:
        rendered.extend([f"other_sections:{title}" for title in other_sections])

    notes = []
    current_title = str((basics.get("current_title") or "")).strip()
    if current_title:
        notes.append(f"CV title was set from the extracted current role: '{current_title}'.")
    if summary_count:
        notes.append(f"Summary was included as {summary_count} concise bullet point{'s' if summary_count != 1 else ''}.")
    if skill_group_count:
        notes.append(f"Technical skills were grouped into {skill_group_count} section{'s' if skill_group_count != 1 else ''} for display.")
    if experience_entries:
        notes.append(f"Work experience was included with {experience_entries} role{'s' if experience_entries != 1 else ''}.")
    else:
        notes.append("Work experience was not included in the output.")
    if project_entries:
        notes.append(f"Projects section includes {project_entries} entr{'ies' if project_entries != 1 else 'y'}.")
    if certification_entries:
        notes.append(f"Certifications were included ({certification_entries}).")
    if language_entries:
        notes.append(f"Languages were included ({language_entries}).")
    if education_entries:
        notes.append("Education was included in the output.")
    else:
        notes.append("Education was not included in the output.")
    if other_sections:
        notes.append("Additional sections included: " + ", ".join(other_sections) + ".")
    if anonymize:
        notes.append("Personal contact details were removed or generalized for anonymized output.")

    result = {
        "current_title_present": bool(current_title),
        "summary_bullet_count": summary_count,
        "skill_group_count": skill_group_count,
        "experience_entries": experience_entries,
        "project_entries": project_entries,
        "education_entries": education_entries,
        "certification_entries": certification_entries,
        "language_entries": language_entries,
        "sections_rendered": rendered,
        "sections_omitted": omitted,
        "other_section_titles": other_sections,
        "content_notes": notes,
        "template_name": template_name,
        "source_suffix": source_path.suffix.lower(),
    }
    if jd_text.strip():
        result["jd_keyword_report"] = _compute_jd_keyword_report(data, jd_text)
    return result

def _translate_non_english(data: dict, model_name: str = "") -> None:
    """Translate non-English content via LLM (no-op if all English)."""
    if not _gemini_api_key or not isinstance(data, dict):
        return
    # Sync MODEL_NAME in cv_engine so translate calls use the configured model
    if model_name:
        core.MODEL_NAME = model_name
    # Full translation pass if significant non-English content detected
    try:
        if hasattr(core, "translate_full_json_via_llm"):
            result = core.translate_full_json_via_llm(data, _gemini_api_key)
            if result:
                core.sanitize_json(data)
    except Exception:
        pass
    # Translate remaining non-English dates and locations
    try:
        if hasattr(core, "translate_dates_via_llm"):
            core.translate_dates_via_llm(data, _gemini_api_key)
    except Exception:
        pass
    try:
        if hasattr(core, "translate_locations_via_llm"):
            core.translate_locations_via_llm(data, _gemini_api_key)
    except Exception:
        pass
    # Final sweep: translate any remaining non-English strings
    try:
        if hasattr(core, "translate_remaining_strings_via_llm"):
            core.translate_remaining_strings_via_llm(data, _gemini_api_key)
    except Exception:
        pass


def configure_gemini(api_key: str) -> None:
    global _gemini_api_key
    if not api_key:
        raise RuntimeError(
            "Gemini API key is not configured. "
            "Set the GEMINI_API_KEY environment variable or visit /setup to enter the key."
        )
    _gemini_api_key = api_key


def resolve_api_key(app_dir: Path, config: dict) -> str:
    """Resolve API key in priority order:
    1. GEMINI_API_KEY environment variable
    2. <app_dir>/.api_key file (set via /setup page)
    3. ~/.quantoricv_settings.json (shared with desktop app)
    """
    env_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if env_key:
        return env_key
    local_key_file = app_dir / ".api_key"
    if local_key_file.exists():
        local_key = local_key_file.read_text(encoding="utf-8").strip()
        if local_key:
            return local_key
    return config.get("gemini_api_key") or config.get("api_key") or ""


def choose_model_name(config: dict) -> str:
    raw = config.get("gemini_model") or config.get("model_name") or "gemini-2.5-flash"
    raw = str(raw).strip()
    if raw.startswith("models/"):
        raw = raw.split("/", 1)[1]
    if raw == "gemini-1.5-flash":
        raw = "gemini-2.5-flash"
    return raw


def call_llm_json(prompt: str, model_name: str):
    client = genai.Client(api_key=_gemini_api_key)
    response = _retry_on_rate_limit(
        lambda: client.models.generate_content(model=model_name, contents=prompt)
    )
    txt = getattr(response, "text", None)
    if not txt:
        raise RuntimeError("Model returned empty response")
    return extract_first_json_object(txt)


def make_temp_workspace() -> Path:
    return Path(tempfile.mkdtemp(prefix="qcv_web_"))


def _safe_source_key_fragment(source_key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(source_key or "")).strip("_") or "source"


class QCVWebEngine:
    def __init__(self, templates_dir: str | Path) -> None:
        self.templates_dir = Path(templates_dir)
        self.app_dir = self.templates_dir.parent
        self.data_dir = Path(os.environ.get("DATA_DIR", str(self.app_dir)))
        self.cache_dir = self.data_dir / "_cache"
        self.config = core.load_config()
        self.model_name = choose_model_name(self.config)
        self.last_content_details = None

    def _status(self, cb: Optional[StatusCallback], name: str, pct: int) -> None:
        if cb:
            cb(name, pct)

    def _debug(self, cb: Optional[Callable[[str], None]], text: str) -> None:
        if cb:
            cb(text)

    def _parse_cv_to_json(self, source_text: str) -> dict:
        master_prompt = self.config["prompt_master_inst"]
        schema = getattr(core, "CV_JSON_SCHEMA", "{}")
        full_prompt = (
            f"{master_prompt}\n\n"
            f"JSON SCHEMA:\n{schema}\n\n"
            f"SOURCE CV TEXT:\n{source_text}"
        )
        data = call_llm_json(full_prompt, self.model_name)
        if hasattr(core, "sanitize_json"):
            data = core.sanitize_json(data)
        _translate_non_english(data, self.model_name)
        return data

    def _parse_cv_file_to_json(self, source_path: Path | str) -> dict:
        source_path = Path(source_path)
        if _is_supported_image_file(source_path):
            master_prompt = self.config["prompt_master_inst"]
            schema = getattr(core, "CV_JSON_SCHEMA", "{}")
            full_prompt = (
                f"{master_prompt}\n\n"
                f"JSON SCHEMA:\n{schema}"
            )
            data = call_llm_json_for_uploaded_file(full_prompt, self.model_name, source_path)
            if hasattr(core, "sanitize_json"):
                data = core.sanitize_json(data)
            _translate_non_english(data, self.model_name)
            return data

        source_text = read_source_text(source_path)
        return self._parse_cv_to_json(source_text)

    def _run_light_check(self, data: dict) -> dict:
        if hasattr(core, "sanitize_json"):
            data = core.sanitize_json(data)
        return data

    def _apply_autofix(self, data: dict) -> dict:
        prompt_autofix = self.config.get("prompt_autofix")
        if not prompt_autofix:
            return data

        qa_report_text = json.dumps(
            {
                "score": 95,
                "missing": ["AutoFix web mode: please repair obvious extraction issues if present"],
                "hallucinations": [],
            },
            ensure_ascii=False,
            indent=2,
        )

        prompt = (
            prompt_autofix
            .replace("{current_json_str}", json.dumps(data, ensure_ascii=False, indent=2))
            .replace("{qa_report_text}", qa_report_text)
        )

        fixed = call_llm_json(prompt, self.model_name)
        if hasattr(core, "sanitize_json"):
            fixed = core.sanitize_json(fixed)
        return fixed

    def _apply_anonymization(self, data: dict) -> dict:
        if hasattr(core, "smart_anonymize_data"):
            api_key = self.config.get("gemini_api_key") or self.config.get("api_key") or ""
            out, _in_tok, _out_tok, _cost = core.smart_anonymize_data(copy.deepcopy(data), api_key, self.config)
            if not isinstance(out, dict):
                out = copy.deepcopy(data)
        else:
            out = copy.deepcopy(data)

        # smart_anonymize_data handles name, contacts, links, companies, publications.
        # Additionally clear location and contact_line for web output.
        basics = out.get("basics", {}) or {}
        basics["location"] = ""
        out["basics"] = basics
        out["location"] = ""
        out["contact_line"] = ""

        return out

    def _check_relevance(self, data: dict, jd_text: str) -> str:
        """Deterministic relevance check: compare CV skills/roles with JD keywords."""
        # Extract all meaningful words from JD (3+ chars, lowercased)
        jd_words = set(w.lower() for w in re.findall(r'[A-Za-z#+.]{3,}', jd_text))

        # Collect candidate's skills, tools, role titles
        cv_terms = set()
        for cat_items in (data.get("skills") or {}).values():
            if isinstance(cat_items, list):
                for s in cat_items:
                    cv_terms.update(w.lower() for w in re.findall(r'[A-Za-z#+.]{3,}', str(s)))
        for exp in (data.get("experience") or []):
            role = str(exp.get("role", ""))
            cv_terms.update(w.lower() for w in re.findall(r'[A-Za-z#+.]{3,}', role))
            for env_item in (exp.get("environment") or []):
                cv_terms.update(w.lower() for w in re.findall(r'[A-Za-z#+.]{3,}', str(env_item)))
        title = str(data.get("basics", {}).get("current_title", ""))
        cv_terms.update(w.lower() for w in re.findall(r'[A-Za-z#+.]{3,}', title))

        jd_words -= _JD_STOP_WORDS
        cv_terms -= _JD_STOP_WORDS

        overlap = jd_words & cv_terms
        if not jd_words or not cv_terms:
            return "MEDIUM"
        jd_ratio = len(overlap) / len(jd_words)
        cv_ratio = len(overlap) / len(cv_terms)
        ratio = max(jd_ratio, cv_ratio)
        print(f"[Relevance] JD: {len(jd_words)}, CV: {len(cv_terms)}, "
              f"overlap: {len(overlap)} (jd={jd_ratio*100:.1f}% cv={cv_ratio*100:.1f}% => max {ratio*100:.1f}%) {overlap}")

        if ratio >= 0.15:
            return "HIGH"
        elif ratio >= 0.05:
            return "MEDIUM"
        else:
            return "LOW"

    def _apply_tailor(self, data: dict, jd_text: str, focus_skills: list | None = None) -> dict:
        """Tailor the extracted CV JSON to match a Job Description."""
        prompt_template = self.config.get("prompt_tailor", core.DEFAULT_PROMPTS.get("prompt_tailor", ""))
        if not prompt_template:
            return data
        input_json_str = json.dumps(data, ensure_ascii=False)
        prompt = prompt_template.replace("{jd_text}", jd_text).replace("{input_json_str}", input_json_str)
        if focus_skills:
            focus_block = "\n\nFOCUS SKILLS (user-selected gaps to address during tailoring):\n- " + "\n- ".join(focus_skills) + "\n\nFor each focus skill: if the candidate has direct or partial experience, emphasize and strengthen the wording. For skills the candidate lacks entirely, ONLY highlight transferable experience from adjacent technologies — NEVER add skills, projects, certifications, or job duties the candidate did not mention in the original CV."
            prompt += focus_block
        raw_data = call_llm_json(prompt, self.model_name)
        cv_data = raw_data.get("cv", raw_data) if isinstance(raw_data, dict) else raw_data
        if hasattr(core, "sanitize_json"):
            cv_data = core.sanitize_json(cv_data)
        return cv_data

    def _analyze_gap(self, data: dict, jd_text: str) -> dict:
        """LLM-based gap analysis: compare CV JSON against JD, return structured assessment."""
        prompt_template = self.config.get(
            "prompt_gap_analysis",
            core.DEFAULT_PROMPTS.get("prompt_gap_analysis", ""),
        )
        if not prompt_template:
            return {}
        cv_json_str = json.dumps(data, ensure_ascii=False)
        prompt = prompt_template.replace("{jd_text}", jd_text).replace("{cv_json}", cv_json_str)
        raw = call_llm_json(prompt, self.model_name)
        # Validate and fill defaults
        result = {
            "match_percentage": int(raw.get("match_percentage", 0)),
            "summary": str(raw.get("summary", "")),
            "strengths": list(raw.get("strengths", [])),
            "weaknesses": list(raw.get("weaknesses", [])),
            "skills_table": [],
        }
        for row in raw.get("skills_table", []):
            if isinstance(row, dict) and "requirement" in row:
                result["skills_table"].append({
                    "requirement": str(row.get("requirement", "")),
                    "category": str(row.get("category", "Nice To Have")),
                    "status": str(row.get("status", "Missing")),
                    "recommendation": str(row.get("recommendation", "")),
                })
        return result

    def _generate_docx(
        self,
        data: dict,
        output_dir: Path,
        base_name: str,
        template_name: str,
        anonymize: bool = False,
        tailor: bool = False,
        debug_cb: Optional[Callable[[str], None]] = None,
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        final_base_name = _build_output_base_name(data, anonymize, tailor=tailor, fallback=base_name)
        result_path = output_dir / f"{final_base_name}.docx"

        if not hasattr(core, "generate_docx_from_json"):
            raise RuntimeError("cv_engine.generate_docx_from_json() not found")

        template_path = self.templates_dir / template_name
        if not template_path.exists():
            raise RuntimeError(f"Template not found: {template_name}")

        fn = core.generate_docx_from_json

        cfg = dict(self.config)
        cfg["active_template"] = template_name
        cfg["template_path"] = str(template_path)
        if not data.get("contact_line"):
            cfg["suppress_contact_line"] = True

        workspace_templates = None
        if cfg.get("workspace_dir"):
            workspace_templates = Path(cfg.get("workspace_dir")) / "templates"

        debug_lines = [
            f"template_name={template_name}",
            f"server_template_path={template_path.resolve()}",
            f"server_template_exists={template_path.exists()}",
            f"cfg.active_template={cfg.get('active_template')}",
        ]
        if workspace_templates is not None:
            wp = workspace_templates / template_name
            debug_lines.append(f"workspace_template_path={wp.resolve()}")
            debug_lines.append(f"workspace_template_exists={wp.exists()}")

        self._debug(debug_cb, "\n".join(debug_lines))

        try:
            sig = inspect.signature(fn)
            argc = len(sig.parameters)
            try:
                if argc == 2:
                    maybe = fn(data, str(result_path))
                elif argc == 3:
                    maybe = fn(data, str(result_path), cfg)
                else:
                    maybe = fn(data, str(result_path), cfg)
            except TypeError:
                maybe = fn(data, result_path, cfg)
        except Exception as e:
            raise RuntimeError(f"DOCX generation failed: {e}") from e

        if isinstance(maybe, (str, Path)) and Path(maybe).exists():
            return Path(maybe)
        if result_path.exists():
            return result_path
        raise RuntimeError("DOCX generation did not produce an output file")

    def _base_json_artifacts_dir(self) -> Path:
        return self.cache_dir / "base_json"

    def _base_json_path(self, source_key: str) -> Path:
        safe_key = _safe_source_key_fragment(source_key)
        return self._base_json_artifacts_dir() / f"{safe_key}.base.json"

    def _save_base_json_artifact(self, source_key: str | None, data: dict) -> None:
        if not source_key:
            return
        artifacts_dir = self._base_json_artifacts_dir()
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        base_json_path = self._base_json_path(source_key)
        base_json_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_base_json_artifact(self, source_key: str | None) -> dict | None:
        if not source_key:
            return None
        base_json_path = self._base_json_path(source_key)
        if not base_json_path.exists():
            return None
        try:
            data = json.loads(base_json_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        return data

    def process(
        self,
        source_path: Path,
        output_dir: Path,
        *,
        anonymize: bool = False,
        autofix: bool = False,
        tailor: bool = False,
        jd_text: str = "",
        force_tailor: bool = False,
        template_name: str,
        source_key: str | None = None,
        status_cb: Optional[StatusCallback] = None,
        debug_cb: Optional[Callable[[str], None]] = None,
        pause_event: Optional[threading.Event] = None,
        gap_ready_cb: Optional[Callable[[dict], None]] = None,
        focus_skills_cb: Optional[Callable[[], list]] = None,
        preloaded_data: dict | None = None,
    ) -> Path:
        source_path = Path(source_path)
        self.last_content_details = None
        self.config = core.load_config()
        self.model_name = choose_model_name(self.config)
        api_key = resolve_api_key(self.app_dir, self.config)
        configure_gemini(api_key)

        if tailor and jd_text.strip():
            jd_err = validate_jd(jd_text)
            if jd_err:
                raise ValueError(jd_err)

        self._status(status_cb, "Uploading", 5)

        if preloaded_data is not None:
            data = preloaded_data
            self._status(status_cb, "Loaded from JSON", 35)
            self._debug(debug_cb, "parse_mode=preloaded_json\nbase_json_reuse=preloaded")
        elif (data := self._load_base_json_artifact(source_key)) is not None:
            self._status(status_cb, "Parsing (reused base JSON)", 20)
            self._debug(
                debug_cb,
                "parse_mode=reused_base_json\n"
                "base_json_reuse=hit\n"
                f"source_key={source_key}",
            )
        else:
            self._status(status_cb, "Parsing (fresh)", 20)
            data = self._parse_cv_file_to_json(source_path)

            self._status(status_cb, "Checking", 45)
            data = self._run_light_check(data)
            self._save_base_json_artifact(source_key, data)
            self._debug(
                debug_cb,
                "parse_mode=fresh_parse\n"
                f"base_json_reuse=miss\nsource_key={source_key}",
            )
        # Store base CV JSON for download
        self._last_base_json = copy.deepcopy(data)

        if autofix:
            self._status(status_cb, "AutoFix", 60)
            data = self._apply_autofix(data)

        if tailor and jd_text.strip():
            # Gap analysis: LLM compares CV vs JD before tailoring
            # Skip if no gap_ready_cb and no pause_event (skip_gap mode)
            skip_gap = (gap_ready_cb is None and pause_event is None)
            try:
                if skip_gap:
                    gap_result = None
                else:
                    self._status(status_cb, "Analyzing fit", 50)
                    gap_result = self._analyze_gap(data, jd_text)
                    gap_result["_output_base"] = _build_output_base_name(data, anonymize=False)
                    self._last_gap_analysis = gap_result
                if gap_ready_cb and gap_result:
                    gap_ready_cb(gap_result, copy.deepcopy(data))
                if pause_event is not None:
                    self._status(status_cb, "gap_analysis_ready", 55)
                    if not pause_event.wait(timeout=600):
                        raise RuntimeError("Gap analysis timed out — user did not proceed within 10 minutes.")
            except (RuntimeError,) as gap_err:
                if "timed out" in str(gap_err):
                    raise
                self._debug(debug_cb, f"Gap analysis failed, proceeding: {gap_err}")

            # Collect user-selected focus skills (if any)
            focus_skills = focus_skills_cb() if focus_skills_cb else []

            self._status(status_cb, "Tailoring to JD", 70)
            data = self._apply_tailor(data, jd_text, focus_skills=focus_skills)
            self._last_tailored_json = copy.deepcopy(data)

        if anonymize:
            self._status(status_cb, "Anonymizing", 75)
            data = self._apply_anonymization(data)

        self.last_content_details = _build_content_details(
            data,
            template_name=template_name,
            anonymize=anonymize,
            source_path=source_path,
            jd_text=jd_text if tailor else "",
        )

        self._status(status_cb, "Generating DOCX", 90)
        result_path = self._generate_docx(data, output_dir, source_path.stem, template_name, anonymize=anonymize, tailor=tailor, debug_cb=debug_cb)

        self._status(status_cb, "Done", 100)
        return result_path

    def refine(
        self,
        tailored_json: dict,
        jd_text: str,
        missing_keywords: list[str],
        output_dir: Path,
        *,
        anonymize: bool = False,
        template_name: str,
        source_name: str = "refined",
        status_cb: Optional[StatusCallback] = None,
        debug_cb: Optional[Callable[[str], None]] = None,
    ) -> Path:
        """Second-pass refinement: weave missing JD keywords into already-tailored CV."""
        self.last_content_details = None
        self.config = core.load_config()
        self.model_name = choose_model_name(self.config)
        api_key = resolve_api_key(self.app_dir, self.config)
        configure_gemini(api_key)

        self._status(status_cb, "Refining", 20)

        prompt_template = self.config.get("prompt_refine", core.DEFAULT_PROMPTS.get("prompt_refine", ""))
        if not prompt_template:
            raise RuntimeError("prompt_refine not configured")

        input_json_str = json.dumps(tailored_json, ensure_ascii=False)
        prompt = (prompt_template
                  .replace("{jd_text}", jd_text)
                  .replace("{missing_keywords}", ", ".join(missing_keywords))
                  .replace("{input_json_str}", input_json_str))

        self._status(status_cb, "Refining (LLM)", 40)
        raw_data = call_llm_json(prompt, self.model_name)
        data = raw_data.get("cv", raw_data) if isinstance(raw_data, dict) else raw_data
        if hasattr(core, "sanitize_json"):
            data = core.sanitize_json(data)
        self._last_tailored_json = copy.deepcopy(data)

        if anonymize:
            self._status(status_cb, "Anonymizing", 70)
            data = self._apply_anonymization(data)

        self.last_content_details = _build_content_details(
            data,
            template_name=template_name,
            anonymize=anonymize,
            source_path=Path(source_name),
            jd_text=jd_text,
        )

        self._status(status_cb, "Generating DOCX", 85)
        result_path = self._generate_docx(data, output_dir, Path(source_name).stem, template_name, anonymize=anonymize, tailor=True, debug_cb=debug_cb)

        self._status(status_cb, "Done", 100)
        return result_path
