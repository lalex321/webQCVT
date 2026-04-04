from __future__ import annotations

import copy
import json
import hashlib
import threading
import time
from pathlib import Path

import os
import httpx

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response as RawResponse
from collections import Counter
from html import escape

from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from converter_engine import InMemoryJobStore, LowRelevanceError, QCVWebEngine, make_temp_workspace, resolve_api_key, _build_output_base_name, choose_model_name, configure_gemini
import cv_engine as _core

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", str(APP_DIR)))
TEMPLATES_DIR = APP_DIR / "templates"
STORE_DIR = DATA_DIR / "_store"
USAGE_LOG = DATA_DIR / "usage_log.jsonl"

TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
STORE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Q-CV Web Converter")
app.mount("/images", StaticFiles(directory=APP_DIR / "images"), name="images")
jobs = InMemoryJobStore()
_SERVER_START = time.time()
_JOB_SEMAPHORE = threading.Semaphore(5)  # max 5 concurrent LLM jobs
_STORE_LOCK = threading.Lock()  # serialize store file writes

import re
_STORE_ID_RE = re.compile(r'^[a-fA-F0-9]+$')

def _validate_store_id(store_id: str) -> None:
    """Reject path traversal and invalid store IDs."""
    if not store_id or not _STORE_ID_RE.match(store_id):
        raise HTTPException(status_code=400, detail="Invalid store ID")

# Background cleanup: remove finished jobs and their tmp dirs every 10 minutes
_JOB_MAX_AGE_SEC = 3600  # 1 hour

def _cleanup_loop():
    import shutil
    while True:
        time.sleep(600)
        try:
            removed = jobs.cleanup_old(_JOB_MAX_AGE_SEC)
            # Clean orphaned tmp dirs older than max age
            tmp_root = Path(os.environ.get("TMPDIR", "/tmp"))
            cutoff = time.time() - _JOB_MAX_AGE_SEC
            for d in tmp_root.glob("qcv_web_*"):
                if d.is_dir() and d.stat().st_mtime < cutoff:
                    shutil.rmtree(d, ignore_errors=True)
        except Exception:
            pass

threading.Thread(target=_cleanup_loop, daemon=True).start()


def _backfill_search_text():
    """One-time migration: add search_text to store entries that lack it."""
    for p in STORE_DIR.glob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            meta = data.get("_meta", {})
            if "search_text" in meta:
                continue
            basics = data.get("basics", {})
            exp = data.get("experience", [])
            skills_text = json.dumps(data.get("skills", {}), ensure_ascii=False).lower()
            meta["search_text"] = " ".join([
                basics.get("name", ""),
                basics.get("current_title", ""),
                exp[0].get("company_name", "") if exp else "",
                meta.get("source_filename", ""),
                meta.get("comments", ""),
                skills_text,
            ]).lower()
            data["_meta"] = meta
            p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            continue

_backfill_search_text()
# Cache will be initialized lazily on first _list_store() call


def append_usage(event: dict) -> None:
    event = dict(event)
    event.setdefault("ts", time.strftime("%Y-%m-%dT%H:%M:%S"))
    with USAGE_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")

def _read_usage_events() -> list[dict]:
    if not USAGE_LOG.exists():
        return []

    events: list[dict] = []
    for line in USAGE_LOG.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            events.append(item)
    return events


_GEMINI_BASE = "https://generativelanguage.googleapis.com"
_PROXY_HOP_HEADERS = {"host", "content-length", "transfer-encoding", "connection"}


async def _proxy_to_gemini(request: Request, path: str) -> RawResponse:
    target_url = f"{_GEMINI_BASE}/{path}"
    body = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in _PROXY_HOP_HEADERS}
    params = dict(request.query_params)
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                params=params,
                content=body,
            )
        resp_headers = {k: v for k, v in resp.headers.items()
                        if k.lower() not in {"transfer-encoding", "content-encoding"}}
        return RawResponse(
            content=resp.content,
            status_code=resp.status_code,
            headers=resp_headers,
            media_type=resp.headers.get("content-type"),
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Proxy error: {e}")


@app.api_route("/v1beta/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def gemini_proxy_v1beta(request: Request, path: str):
    """Proxy for Gemini API v1beta calls (generate content, file get/delete)."""
    return await _proxy_to_gemini(request, f"v1beta/{path}")


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def gemini_proxy_v1(request: Request, path: str):
    """Proxy for Gemini API v1 (stable) calls."""
    return await _proxy_to_gemini(request, f"v1/{path}")


@app.api_route("/v1alpha/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def gemini_proxy_v1alpha(request: Request, path: str):
    """Proxy for Gemini API v1alpha (experimental) calls."""
    return await _proxy_to_gemini(request, f"v1alpha/{path}")


@app.api_route("/upload/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def gemini_proxy_upload(request: Request, path: str):
    """Proxy for Gemini Files API uploads."""
    return await _proxy_to_gemini(request, f"upload/{path}")


@app.get("/admin/usage", response_class=HTMLResponse)
def admin_usage():
    events = _read_usage_events()
    total_jobs = sum(1 for e in events if e.get("event") == "started")
    done_jobs = sum(1 for e in events if e.get("event") == "done")
    failed_jobs = sum(1 for e in events if e.get("event") == "failed")
    unique_ips = len({str(e.get("ip") or "") for e in events if e.get("ip")})

    template_counter = Counter(
        str(e.get("template") or "")
        for e in events
        if e.get("event") == "started" and e.get("template")
    )
    top_templates = template_counter.most_common(10)
    recent_events = list(reversed(events))

    rows = []
    for item in recent_events:
        rows.append(
            "<tr>"
            f"<td>{escape(str(item.get('ts', '')))}</td>"
            f"<td>{escape(str(item.get('event', '')))}</td>"
            f"<td>{escape(str(item.get('job_id', ''))[:8])}</td>"
            f"<td>{escape(str(item.get('ip', '')))}</td>"
            f"<td>{escape(str(item.get('file', '')))}</td>"
            f"<td>{escape(str(item.get('template', '')))}</td>"
            f"<td>{escape(str(item.get('anonymize', '')))}</td>"
            f"<td>{escape(str(item.get('autofix', '')))}</td>"
            f"<td>{escape(str(item.get('duration_sec', '')))}</td>"
            f"<td>{escape(str(item.get('error', '')))}</td>"
            "</tr>"
        )

    top_templates_html = "".join(
        f"<li><strong>{escape(name)}</strong> — {count}</li>"
        for name, count in top_templates
    ) or "<li>No data</li>"

    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Q-CV Usage Admin</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
    h1 {{ margin-bottom: 8px; }}
    .stats {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 18px 0 24px; }}
    .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px 14px; min-width: 140px; }}
    .card .v {{ font-size: 24px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f5f5f5; position: sticky; top: 0; }}
    .table-wrap {{ overflow: auto; max-height: 70vh; }}
    ul {{ margin-top: 8px; }}
  </style>
</head>
<body>
  <h1>Q-CV Usage</h1>
  <div class="stats">
    <div class="card"><div>Total jobs</div><div class="v">{total_jobs}</div></div>
    <div class="card"><div>Done</div><div class="v">{done_jobs}</div></div>
    <div class="card"><div>Failed</div><div class="v">{failed_jobs}</div></div>
    <div class="card"><div>Unique IPs</div><div class="v">{unique_ips}</div></div>
  </div>

  <h2>Top templates</h2>
  <ul>{top_templates_html}</ul>

  <h2>All log entries</h2>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>ts</th><th>event</th><th>job</th><th>ip</th><th>file</th><th>template</th>
          <th>anon</th><th>autofix</th><th>sec</th><th>error</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
    return HTMLResponse(html)


@app.get("/admin/prompts")
def get_prompts():
    cfg = _core.load_config()
    prompts = {k: cfg[k] for k in cfg if k.startswith("prompt_")}
    defaults = dict(_core.DEFAULT_PROMPTS)
    return {"prompts": prompts, "defaults": defaults}


@app.put("/admin/prompts/{key}")
async def save_prompt(key: str, request: Request):
    if not key.startswith("prompt_"):
        raise HTTPException(status_code=400, detail="Invalid prompt key")
    body = await request.json()
    cfg = _core.load_config()
    cfg[key] = body["text"]
    _core.save_config(cfg)
    return {"ok": True}


@app.delete("/admin/prompts/{key}")
def reset_prompt(key: str):
    if key not in _core.DEFAULT_PROMPTS:
        raise HTTPException(status_code=404, detail="Unknown prompt key")
    cfg = _core.load_config()
    cfg[key] = _core.DEFAULT_PROMPTS[key]
    _core.save_config(cfg)
    return {"ok": True, "text": cfg[key]}


## ── Store endpoints (Batch tab) ──────────────────────────────────────

@app.get("/store")
def list_store():
    return {"items": _list_store()}


@app.get("/store/{store_id}")
def get_store_item(store_id: str):
    """Return full stored CV including tailor session if present."""
    _validate_store_id(store_id)
    p = STORE_DIR / f"{store_id}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Not found")
    data = json.loads(p.read_text(encoding="utf-8"))
    return data


@app.delete("/store/{store_id}")
def delete_store_item(store_id: str):
    _validate_store_id(store_id)
    p = STORE_DIR / f"{store_id}.json"
    if not p.exists():
        raise HTTPException(status_code=404, detail="Not found")
    p.unlink()
    _store_cache_remove(store_id)
    return {"ok": True}


_EDITABLE_META_FIELDS = {"comments"}

@app.patch("/store/{store_id}/meta")
async def update_store_meta(store_id: str, request: Request):
    _validate_store_id(store_id)
    body = await request.json()
    field = body.get("field", "")
    value = body.get("value", "")
    if field not in _EDITABLE_META_FIELDS:
        raise HTTPException(status_code=400, detail=f"Field not editable: {field}")
    with _STORE_LOCK:
        p = STORE_DIR / f"{store_id}.json"
        if not p.exists():
            raise HTTPException(status_code=404, detail="Not found")
        data = json.loads(p.read_text(encoding="utf-8"))
        data["_meta"][field] = value
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        cached = _store_cache_get_meta(store_id)
        if cached:
            cached[field] = value
    return {"ok": True}


@app.post("/store/batch")
async def batch_store_action(request: Request):
    body = await request.json()
    action = body.get("action")
    ids = body.get("ids", [])
    template_name = body.get("template_name", "")
    jd_text = body.get("jd_text", "")
    anonymize = body.get("anonymize", False)

    if not ids:
        raise HTTPException(status_code=400, detail="No IDs provided")

    for sid in ids:
        _validate_store_id(sid)

    if action == "delete":
        deleted = 0
        for sid in ids:
            p = STORE_DIR / f"{sid}.json"
            if p.exists():
                p.unlink()
                _store_cache_remove(sid)
                deleted += 1
        return {"ok": True, "deleted": deleted}

    if action not in ("generate", "anonymize", "tailor"):
        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")

    if action == "tailor" and not jd_text.strip():
        raise HTTPException(status_code=400, detail="JD text is required for tailoring")

    if not template_name:
        tpls = sorted(TEMPLATES_DIR.glob("*.docx"))
        template_name = tpls[0].name if tpls else ""
    if not (TEMPLATES_DIR / template_name).exists():
        raise HTTPException(status_code=400, detail=f"Unknown template: {template_name}")

    client_ip = request.client.host if request.client else "unknown"
    created_jobs = []

    for sid in ids:
        cv_json = _load_store_cv(sid)
        if not cv_json:
            continue
        workdir = make_temp_workspace()
        # Write a dummy source so the pipeline has a path
        dummy = workdir / "batch_cv.json"
        dummy.write_text(json.dumps(cv_json, ensure_ascii=False), encoding="utf-8")

        do_anon = anonymize or (action == "anonymize")
        do_tailor = action == "tailor"

        job = jobs.create(f"batch_{sid[:8]}.json", anonymize=do_anon, autofix=False, template_name=template_name)
        started_at = time.time()

        thread = threading.Thread(
            target=_run_job,
            args=(job.job_id, dummy, workdir, do_anon, False, do_tailor, jd_text, False, template_name, sid, client_ip, started_at, True),
            kwargs={"preloaded_data": cv_json},
            daemon=True,
        )
        thread.start()
        created_jobs.append({"store_id": sid, "job_id": job.job_id})

    return {"ok": True, "jobs": created_jobs}


@app.get("/setup", response_class=HTMLResponse)
def setup_page():
    cfg = _core.load_config()
    current_key = resolve_api_key(APP_DIR, cfg)
    key_source = "not set"
    if os.environ.get("GEMINI_API_KEY", "").strip():
        key_source = "environment variable <code>GEMINI_API_KEY</code>"
    elif (APP_DIR / ".api_key").exists() and (APP_DIR / ".api_key").read_text().strip():
        key_source = "local <code>.api_key</code> file"
    elif cfg.get("gemini_api_key") or cfg.get("api_key"):
        key_source = "<code>~/.quantoricv_settings.json</code>"

    key_display = "[configured]" if current_key else "(not set)"
    status_color = "#2e7d32" if current_key else "#c62828"
    status_text = f"Key {key_display} — source: {key_source}" if current_key else "⚠️ No API key configured"

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Q-CV Setup</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 520px; margin: 60px auto; color: #222; }}
    h1 {{ margin-bottom: 4px; }}
    .status {{ padding: 10px 14px; border-radius: 6px; background: #f5f5f5; color: {status_color};
               font-size: 14px; margin: 16px 0 24px; }}
    label {{ display: block; font-weight: bold; margin-bottom: 6px; }}
    input[type=text] {{ width: 100%; padding: 8px 10px; font-size: 14px; border: 1px solid #ccc;
                        border-radius: 4px; box-sizing: border-box; }}
    button {{ margin-top: 12px; padding: 9px 22px; background: #1565c0; color: #fff;
              border: none; border-radius: 4px; font-size: 14px; cursor: pointer; }}
    button:hover {{ background: #0d47a1; }}
    .note {{ margin-top: 20px; font-size: 12px; color: #666; }}
    a {{ color: #1565c0; }}
  </style>
</head>
<body>
  <h1>Q-CV Setup</h1>
  <div class="status">{status_text}</div>
  <form method="post" action="/setup">
    <label for="key">Gemini API Key</label>
    <input type="text" id="key" name="api_key" placeholder="AIza..." autocomplete="off">
    <button type="submit">Save &amp; Apply</button>
  </form>
  <p class="note">
    The key is saved to <code>.api_key</code> in the app directory and takes effect immediately —
    no server restart needed.<br><br>
    Priority: <code>GEMINI_API_KEY</code> env var &gt; <code>.api_key</code> file &gt; <code>~/.quantoricv_settings.json</code><br><br>
    <a href="/">← Back to converter</a>
  </p>
</body>
</html>"""
    return HTMLResponse(html)


@app.post("/setup")
async def setup_save(api_key: str = Form(...)):
    key = api_key.strip()
    if not key:
        raise HTTPException(status_code=400, detail="API key cannot be empty")
    (APP_DIR / ".api_key").write_text(key, encoding="utf-8")
    return RedirectResponse(url="/setup?saved=1", status_code=303)


@app.get("/", response_class=HTMLResponse)
def index():
    index_path = APP_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return index_path.read_text(encoding="utf-8")


@app.get("/stats")
def server_stats():
    today = time.strftime("%Y-%m-%d")
    events = _read_usage_events()
    today_done = sum(1 for e in events if e.get("event") == "done" and e.get("ts", "").startswith(today))
    today_failed = sum(1 for e in events if e.get("event") == "failed" and e.get("ts", "").startswith(today))
    total_done = sum(1 for e in events if e.get("event") == "done")
    uptime_sec = int(time.time() - _SERVER_START)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime_str = f"{h}h {m}m" if h else f"{m}m {s}s"
    return {
        "active_jobs": jobs.active_count(),
        "today_processed": today_done,
        "today_failed": today_failed,
        "total_processed": total_done,
        "uptime": uptime_str,
    }


@app.get("/templates")
def list_templates():
    if not TEMPLATES_DIR.exists():
        return {"templates": []}
    names = sorted([p.name for p in TEMPLATES_DIR.glob("*.docx") if p.is_file()])
    return {"templates": names}


def build_source_key(source_path: Path) -> str:
    h = hashlib.sha256()
    with source_path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _build_processing_details(
    source_name: str,
    source_path: Path,
    template_name: str,
    anonymize: bool,
    autofix: bool,
    output_path: Path | None = None,
    content_details: dict | None = None,
) -> dict:
    suffix = source_path.suffix.lower()
    source_type = {
        ".pdf": "PDF",
        ".docx": "DOCX",
        ".png": "PNG",
        ".jpg": "JPG",
        ".jpeg": "JPEG",
    }.get(suffix, suffix.lstrip(".").upper() or "Unknown")

    details = {
        "source_type": source_type,
        "source_file": source_name,
        "template": template_name,
        "anonymize": bool(anonymize),
        "autofix": bool(autofix),
        "reuse_enabled": True,
        "image_input": suffix in {".png", ".jpg", ".jpeg"},
        "output_generated": output_path is not None,
        "output_file": output_path.name if output_path else None,
    }
    if content_details:
        details["content_details"] = content_details
    return details


## ── CV Store helpers ─────────────────────────────────────────────────

# In-memory cache of _meta dicts — avoids reading all JSON files on every /store request
_store_cache: list[dict] = []
_store_cache_ready = False

def _store_cache_init():
    """Load all _meta from store files into cache. Called once at startup."""
    global _store_cache_ready
    for p in STORE_DIR.glob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            meta = data.get("_meta", {})
            meta["id"] = p.stem
            _store_cache.append(meta)
        except Exception:
            continue
    _store_cache.sort(key=lambda m: m.get("date", ""), reverse=True)
    _store_cache_ready = True

def _store_cache_upsert(meta: dict):
    """Add or update a meta entry in the cache."""
    sid = meta.get("id", "")
    for i, m in enumerate(_store_cache):
        if m.get("id") == sid:
            _store_cache[i] = meta
            return
    _store_cache.append(meta)

def _store_cache_remove(store_id: str):
    """Remove an entry from the cache."""
    global _store_cache
    _store_cache = [m for m in _store_cache if m.get("id") != store_id]

def _store_cache_get_meta(store_id: str) -> dict | None:
    """Get cached meta by ID."""
    for m in _store_cache:
        if m.get("id") == store_id:
            return m
    return None


def _find_store_by_name(name: str) -> Path | None:
    """Check if a CV with this name already exists in store."""
    if not name:
        return None
    for p in STORE_DIR.glob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if data.get("_meta", {}).get("name", "").lower() == name.lower():
                return p
        except Exception:
            continue
    return None


def _save_to_store(store_id: str, cv_json: dict, source_filename: str) -> None:
    """Persist extracted CV JSON with metadata to _store/."""
    with _STORE_LOCK:
        basics = cv_json.get("basics", {})
        existing = _find_store_by_name(basics.get("name", ""))
        if existing and existing.stem != store_id:
            return
        exp = cv_json.get("experience", [])
        # Auto-detect source (like desktop Q-CV)
        fname_lower = (source_filename or "").lower()
        links_dump = json.dumps(basics.get("links", [])).lower()
        if "linkedin.com" in links_dump or "linkedin" in fname_lower or fname_lower.startswith("profile"):
            comments = "Source: LinkedIn"
        else:
            comments = ""

        # Build search index (like desktop Q-CV: name, title, company, skills, filename, comments)
        skills_text = json.dumps(cv_json.get("skills", {}), ensure_ascii=False).lower()
        search_text = " ".join([
            basics.get("name", ""),
            basics.get("current_title", ""),
            exp[0].get("company_name", "") if exp else "",
            source_filename or "",
            comments,
            skills_text,
        ]).lower()

        meta = {
            "id": store_id,
            "name": basics.get("name", ""),
            "role": basics.get("current_title", ""),
            "company": exp[0].get("company_name", "") if exp else "",
            "date": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "source_filename": source_filename,
            "comments": comments,
            "search_text": search_text,
        }
        data = {"_meta": meta, **{k: v for k, v in cv_json.items() if k != "_meta"}}
        (STORE_DIR / f"{store_id}.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        _store_cache_upsert(dict(meta))


def _save_store_gap(store_id: str, gap_analysis: dict, jd_text: str, base_json: dict = None) -> None:
    """Save gap analysis to store entry — sets 'analyzed' badge."""
    with _STORE_LOCK:
        p = STORE_DIR / f"{store_id}.json"
        if not p.exists():
            name = (base_json or {}).get("basics", {}).get("name", "")
            p = _find_store_by_name(name)
            if not p:
                return
        data = json.loads(p.read_text(encoding="utf-8"))
        data["_gap_session"] = {
            "gap_analysis": gap_analysis,
            "jd_text": jd_text,
            "date": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        data["_meta"]["analyzed"] = True
        data["_meta"]["match_pct"] = int(gap_analysis.get("match_percentage", 0))
        data["_meta"]["date"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        data["_meta"]["id"] = p.stem
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        _store_cache_upsert(dict(data["_meta"]))


def _update_store_tailor(store_id: str, tailored_json: dict, jd_text: str,
                         gap_analysis: dict, focus_skills: list,
                         keyword_report: dict) -> None:
    """Update an existing store entry with tailoring session data."""
    with _STORE_LOCK:
        p = STORE_DIR / f"{store_id}.json"
        if not p.exists():
            name = tailored_json.get("basics", {}).get("name", "")
            p = _find_store_by_name(name)
            if not p:
                return
        data = json.loads(p.read_text(encoding="utf-8"))
        data["_tailor_session"] = {
            "tailored_json": tailored_json,
            "jd_text": jd_text,
            "gap_analysis": gap_analysis,
            "focus_skills": focus_skills,
            "keyword_report": keyword_report,
            "date": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    data["_meta"]["tailored"] = True
    data["_meta"]["date"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    # Keep gap analysis match_percentage (consistent with Fit Report on Convert tab)
    if gap_analysis and gap_analysis.get("match_percentage"):
        data["_meta"]["match_pct"] = int(gap_analysis["match_percentage"])
    data["_meta"]["id"] = p.stem
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _store_cache_upsert(dict(data["_meta"]))


def _list_store() -> list[dict]:
    """Return list of _meta dicts for all stored CVs (from cache)."""
    if not _store_cache_ready:
        _store_cache_init()
    return sorted(_store_cache, key=lambda m: m.get("date", ""), reverse=True)


def _load_store_cv(store_id: str) -> dict | None:
    """Load a stored CV JSON, stripping _meta."""
    p = STORE_DIR / f"{store_id}.json"
    if not p.exists():
        return None
    data = json.loads(p.read_text(encoding="utf-8"))
    data.pop("_meta", None)
    return data


def _run_job(job_id: str, source_path: Path, workdir: Path, anonymize: bool, autofix: bool, tailor: bool, jd_text: str, force_tailor: bool, template_name: str, source_key: str | None, client_ip: str, started_at: float, skip_gap: bool = False, preloaded_focus_skills: list | None = None, preloaded_data: dict | None = None, preloaded_gap: dict | None = None) -> None:
    jobs.update(job_id, status="Queued", progress=0)
    _JOB_SEMAPHORE.acquire()
    try:
        def cb(status: str, progress: int) -> None:
            jobs.update(job_id, status=status, progress=progress)

        def dbg(text: str) -> None:
            jobs.update(job_id, debug=text)

        # Create pause event for gap analysis (tailor jobs only, unless skip_gap)
        pause_event = None
        gap_ready_cb = None
        focus_skills_cb = None
        if tailor and jd_text.strip() and not skip_gap:
            pause_event = threading.Event()

            def gap_ready_cb(gap_result: dict, base_json: dict = None) -> None:
                job = jobs.get(job_id)
                if job:
                    setattr(job, "_gap_analysis", gap_result)
                    setattr(job, "_pause_event", pause_event)
                    if base_json:
                        setattr(job, "_cv_json", base_json)
                        # Early save to store — CV available before DOCX generation
                        try:
                            sid = source_key or hashlib.sha256(
                                json.dumps(base_json, sort_keys=True).encode()
                            ).hexdigest()
                            if not (STORE_DIR / f"{sid}.json").exists():
                                _save_to_store(sid, base_json, source_path.name)
                            # Save gap analysis immediately (shows "Analyzed" badge)
                            _save_store_gap(sid, gap_result, jd_text, base_json)
                        except Exception:
                            pass

            def focus_skills_cb() -> list:
                job = jobs.get(job_id)
                if job and getattr(job, "_cancelled", False):
                    raise RuntimeError("Job cancelled by user")
                return getattr(job, "_focus_skills", []) if job else []

        # skip_gap: pre-fill focus_skills and gap_analysis, no pause
        if skip_gap and tailor:
            _preloaded_fs = preloaded_focus_skills or []
            def focus_skills_cb() -> list:
                return _preloaded_fs
            # Preserve gap_analysis and focus_skills on job for store save
            job = jobs.get(job_id)
            if job:
                if preloaded_gap:
                    setattr(job, "_gap_analysis", preloaded_gap)
                setattr(job, "_focus_skills", _preloaded_fs)

        job_engine = QCVWebEngine(TEMPLATES_DIR)
        result_path = job_engine.process(
            source_path=source_path,
            output_dir=workdir,
            anonymize=anonymize,
            autofix=autofix,
            tailor=tailor,
            jd_text=jd_text,
            force_tailor=force_tailor,
            template_name=template_name,
            source_key=source_key,
            status_cb=cb,
            debug_cb=dbg,
            pause_event=pause_event,
            gap_ready_cb=gap_ready_cb,
            focus_skills_cb=focus_skills_cb,
            preloaded_data=preloaded_data,
        )

        # Store base CV JSON on job for download
        job = jobs.get(job_id)
        base_json = getattr(job_engine, "_last_base_json", None)
        if base_json and job:
            setattr(job, "_cv_json", base_json)

        if job:
            details = _build_processing_details(
                source_name=getattr(job, "filename", source_path.name),
                source_path=source_path,
                template_name=template_name,
                anonymize=anonymize,
                autofix=autofix,
                output_path=result_path,
                content_details=getattr(job_engine, "last_content_details", None),
            )
            setattr(job, "details", details)
            # Store data for potential refine pass
            if tailor and jd_text.strip():
                setattr(job, "_tailored_json", getattr(job_engine, "_last_tailored_json", None))
                setattr(job, "_jd_text", jd_text)
                setattr(job, "_output_dir", str(workdir))
                setattr(job, "_source_name", source_path.name)

        jobs.update(job_id, status="Done", progress=100, result_path=str(result_path))

        # Auto-save base CV JSON to persistent store
        try:
            job = jobs.get(job_id)
            if base_json:
                sid = source_key or hashlib.sha256(
                    json.dumps(base_json, sort_keys=True).encode()
                ).hexdigest()
                if not (STORE_DIR / f"{sid}.json").exists():
                    _save_to_store(sid, base_json, source_path.name)
                # Save tailoring session if tailor was performed
                if tailor and jd_text.strip():
                    tailored = getattr(job_engine, "_last_tailored_json", None)
                    gap = getattr(job, "_gap_analysis", None) if job else None
                    focus = getattr(job, "_focus_skills", []) if job else []
                    cd = (getattr(job, "details", None) or {}).get("content_details") or {}
                    kw_report = cd.get("jd_keyword_report", {})
                    if tailored:
                        _update_store_tailor(sid, tailored, jd_text, gap or {}, focus, kw_report)
        except Exception:
            pass

        append_usage({
            "event": "done",
            "job_id": job_id,
            "ip": client_ip,
            "file": source_path.name,
            "template": template_name,
            "anonymize": anonymize,
            "autofix": autofix,
            "tailor": tailor,
            "duration_sec": round(time.time() - started_at, 2),
        })
    except LowRelevanceError as e:
        jobs.update(job_id, status="Low Relevance", progress=100, error=str(e))
        append_usage({
            "event": "skipped_low_relevance",
            "job_id": job_id, "ip": client_ip,
            "file": source_path.name, "tailor": True,
            "duration_sec": round(time.time() - started_at, 2),
        })
    except Exception as e:
        jobs.update(job_id, status="Failed", progress=100, error=str(e))
        append_usage({
            "event": "failed",
            "job_id": job_id,
            "ip": client_ip,
            "file": source_path.name,
            "template": template_name,
            "anonymize": anonymize,
            "autofix": autofix,
            "tailor": tailor,
            "duration_sec": round(time.time() - started_at, 2),
            "error": str(e),
        })
    finally:
        _JOB_SEMAPHORE.release()


@app.post("/jobs")
async def create_job(
    request: Request,
    file: UploadFile = File(...),
    anonymize: bool = Form(False),
    autofix: bool = Form(False),
    tailor: bool = Form(False),
    jd_text: str = Form(""),
    template_name: str = Form(...),
    force_tailor: bool = Form(False),
    skip_gap: bool = Form(False),
    focus_skills_json: str = Form(""),
    import_only: bool = Form(False),
    store_id: str = Form(""),
):
    suffix = Path(file.filename or "upload.docx").suffix.lower()
    if suffix not in {".pdf", ".docx", ".png", ".jpg", ".jpeg", ".json"}:
        raise HTTPException(status_code=400, detail="Only PDF, DOCX, PNG, JPG, JPEG, and JSON are supported.")

    if not template_name:
        raise HTTPException(status_code=400, detail="Template is required.")

    template_path = TEMPLATES_DIR / template_name
    if not template_path.exists():
        raise HTTPException(status_code=400, detail=f"Unknown template: {template_name}")

    # Read uploaded file
    workdir = make_temp_workspace()
    source_path = workdir / (file.filename or "uploaded_file")
    with source_path.open("wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)

    # Handle JSON CV upload: extract CV data and optional _fit_session
    preloaded_data = None
    fit_session = None
    if suffix == ".json":
        try:
            raw = json.loads(source_path.read_text(encoding="utf-8"))
            fit_session = raw.pop("_fit_session", None)
            preloaded_data = raw  # remaining dict is the CV JSON
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise HTTPException(status_code=400, detail=f"Invalid JSON file: {exc}")

        # If _fit_session contains JD and user didn't provide one, use it
        if fit_session and not jd_text.strip():
            jd_text = fit_session.get("jd_text", "")
        # Auto-enable tailor if fit_session has JD
        if fit_session and fit_session.get("jd_text", "").strip():
            tailor = True
        # Extract focus_skills from fit_session (only when frontend requested skip_gap)
        if fit_session and skip_gap:
            user_edits = fit_session.get("user_edits", {})
            if not focus_skills_json and user_edits.get("checked_skills"):
                focus_skills_json = json.dumps(user_edits["checked_skills"])

    if tailor and not jd_text.strip():
        raise HTTPException(status_code=400, detail="Job description is required when tailoring is enabled.")

    source_key = build_source_key(source_path) if suffix != ".json" else None
    # For CVs loaded from store (JSON), use store_id as source_key
    if not source_key and store_id:
        source_key = store_id

    # Skip if already in store (batch import dedup only)
    if import_only and source_key and (STORE_DIR / f"{source_key}.json").exists():
        return {"job_id": "skip", "status": "Done", "progress": 100, "already_in_store": True}

    job = jobs.create(
        file.filename or "uploaded_file",
        anonymize=anonymize,
        autofix=autofix,
        template_name=template_name,
    )

    details = _build_processing_details(
        source_name=file.filename or source_path.name,
        source_path=source_path,
        template_name=template_name,
        anonymize=anonymize,
        autofix=autofix,
        output_path=None,
        content_details=None,
    )
    setattr(job, "details", details)

    client_ip = request.client.host if request.client else "unknown"
    started_at = time.time()
    append_usage({
        "event": "started",
        "job_id": job.job_id,
        "ip": client_ip,
        "file": source_path.name,
        "template": template_name,
        "anonymize": anonymize,
        "autofix": autofix,
        "tailor": tailor,
        "size_bytes": source_path.stat().st_size if source_path.exists() else None,
    })

    thread = threading.Thread(
        target=_run_job,
        args=(job.job_id, source_path, workdir, anonymize, autofix, tailor, jd_text, force_tailor, template_name, source_key, client_ip, started_at, skip_gap),
        kwargs={
            "preloaded_focus_skills": json.loads(focus_skills_json) if focus_skills_json else None,
            "preloaded_data": preloaded_data,
            "preloaded_gap": fit_session.get("gap_analysis") if fit_session else None,
        },
        daemon=True,
    )
    thread.start()

    return {
        "job_id": job.job_id,
        "status": job.status,
        "progress": job.progress,
        "filename": job.filename,
        "template": template_name,
        "details": getattr(job, "details", None),
    }


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    resp = {
        "job_id": job.job_id,
        "filename": job.filename,
        "status": job.status,
        "progress": job.progress,
        "error": job.error,
        "debug": getattr(job, "debug", ""),
        "ready": bool(job.result_path),
        "details": getattr(job, "details", None),
    }
    gap = getattr(job, "_gap_analysis", None)
    if gap:
        resp["gap_analysis"] = gap
    return resp


@app.get("/jobs/{job_id}/cv_json")
def get_cv_json(job_id: str):
    """Return the extracted base CV JSON for this job."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    cv_json = getattr(job, "_cv_json", None)
    if not cv_json:
        raise HTTPException(status_code=404, detail="CV JSON not available yet")
    return cv_json


@app.put("/jobs/{job_id}/cv_json")
async def update_cv_json(job_id: str, request: Request):
    """Update the base CV JSON for this job (from the editor)."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    body = await request.json()
    # Backup previous version
    prev = getattr(job, "_cv_json", None)
    if prev:
        bak_list = getattr(job, "_cv_json_bak", [])
        bak_list.append(copy.deepcopy(prev))
        setattr(job, "_cv_json_bak", bak_list)
    setattr(job, "_cv_json", body)
    return {"ok": True}


@app.post("/jobs/{job_id}/reanalyze")
async def reanalyze_job(job_id: str, request: Request):
    """Re-run gap analysis on the (possibly edited) CV JSON."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    cv_json = getattr(job, "_cv_json", None)
    if not cv_json:
        raise HTTPException(status_code=400, detail="No CV JSON available")
    body = await request.json()
    jd_text = body.get("jd_text", "")
    if not jd_text.strip():
        raise HTTPException(status_code=400, detail="Job description is required")

    engine = QCVWebEngine(TEMPLATES_DIR)
    engine.config = _core.load_config()
    engine.model_name = choose_model_name(engine.config)
    api_key = resolve_api_key(engine.app_dir, engine.config)
    configure_gemini(api_key)

    gap_result = engine._analyze_gap(cv_json, jd_text)
    gap_result["_output_base"] = _build_output_base_name(cv_json, anonymize=False)

    setattr(job, "_gap_analysis", gap_result)
    return gap_result


@app.get("/jobs/{job_id}/download")
def download_job_result(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "Done" or not job.result_path:
        raise HTTPException(status_code=400, detail="Result is not ready yet")

    result_path = Path(job.result_path)
    if not result_path.exists():
        raise HTTPException(status_code=404, detail="Output file missing")

    return FileResponse(
        path=result_path,
        filename=result_path.name,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


def _run_refine(job_id: str, tailored_json: dict, jd_text: str, missing_keywords: list[str],
                output_dir: str, anonymize: bool, template_name: str, source_name: str,
                client_ip: str, started_at: float) -> None:
    try:
        def cb(status: str, progress: int) -> None:
            jobs.update(job_id, status=status, progress=progress)

        def dbg(text: str) -> None:
            jobs.update(job_id, debug=text)

        engine = QCVWebEngine(TEMPLATES_DIR)
        result_path = engine.refine(
            tailored_json=tailored_json,
            jd_text=jd_text,
            missing_keywords=missing_keywords,
            output_dir=Path(output_dir),
            anonymize=anonymize,
            template_name=template_name,
            source_name=source_name,
            status_cb=cb,
            debug_cb=dbg,
        )

        job = jobs.get(job_id)
        if job:
            details = _build_processing_details(
                source_name=source_name,
                source_path=Path(source_name),
                template_name=template_name,
                anonymize=anonymize,
                autofix=False,
                output_path=result_path,
                content_details=getattr(engine, "last_content_details", None),
            )
            setattr(job, "details", details)
            # Update stored tailored JSON for potential further refines
            setattr(job, "_tailored_json", getattr(engine, "_last_tailored_json", None) or tailored_json)

        jobs.update(job_id, status="Done", progress=100, result_path=str(result_path))
    except Exception as e:
        jobs.update(job_id, status="Failed", progress=100, error=str(e))


@app.post("/jobs/{job_id}/refine")
async def refine_job(job_id: str, request: Request):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "Done":
        raise HTTPException(status_code=400, detail="Job must be in Done state to refine")

    tailored_json = getattr(job, "_tailored_json", None)
    jd_text = getattr(job, "_jd_text", None)
    output_dir = getattr(job, "_output_dir", None)
    source_name = getattr(job, "_source_name", "refined")

    if not tailored_json or not jd_text:
        raise HTTPException(status_code=400, detail="No tailoring data available for this job")

    # Get missing keywords from current keyword report
    details = getattr(job, "details", None) or {}
    cd = details.get("content_details") or {}
    kw_report = cd.get("jd_keyword_report") or {}
    missing = kw_report.get("missing", [])
    if not missing:
        raise HTTPException(status_code=400, detail="No missing keywords to refine")

    # Reset job status for refine pass — clear result_path so polling doesn't see it as ready
    jobs.update(job_id, status="Refining", progress=0, error=None, result_path="")

    client_ip = request.client.host if request.client else "unknown"
    thread = threading.Thread(
        target=_run_refine,
        args=(job_id, tailored_json, jd_text, missing, output_dir,
              job.anonymize, job.template_name, source_name,
              client_ip, time.time()),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "status": "Refining"}


@app.post("/jobs/{job_id}/continue")
async def continue_job(job_id: str, request: Request):
    """Unblock a job paused at gap_analysis_ready to proceed with tailoring."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "gap_analysis_ready":
        raise HTTPException(status_code=400, detail="Job is not waiting for continuation")

    pause_event = getattr(job, "_pause_event", None)
    if not pause_event:
        raise HTTPException(status_code=400, detail="No pause event found")

    # Store focus skills selected by user for the tailor prompt
    try:
        body = await request.json()
        focus_skills = body.get("focus_skills", [])
    except Exception:
        focus_skills = []
    if focus_skills:
        setattr(job, "_focus_skills", focus_skills)
    pause_event.set()
    return {"job_id": job_id, "status": "Resuming"}


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a pending job — unblocks pause_event so the thread can exit."""
    job = jobs.get(job_id)
    if not job:
        return {"ok": True}  # already gone
    # Set cancelled flag so gap analysis raises after unblock
    setattr(job, "_cancelled", True)
    # Unblock pause_event if waiting
    pause_event = getattr(job, "_pause_event", None)
    if pause_event:
        pause_event.set()
    # Mark as failed
    if job.status not in ("Done", "Failed"):
        jobs.update(job_id, status="Failed", progress=100, error="Cancelled by user")
    return {"ok": True}
