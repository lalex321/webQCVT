from __future__ import annotations

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

from converter_engine import InMemoryJobStore, QCVWebEngine, make_temp_workspace, resolve_api_key
import cv_engine as _core

APP_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = APP_DIR / "templates"
USAGE_LOG = APP_DIR / "usage_log.jsonl"

TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Q-CV Web Converter")
app.mount("/images", StaticFiles(directory=APP_DIR / "images"), name="images")
jobs = InMemoryJobStore()


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

    key_display = f"{current_key[:8]}...{current_key[-4:]}" if len(current_key) > 12 else ("(not set)" if not current_key else current_key)
    status_color = "#2e7d32" if current_key else "#c62828"
    status_text = f"Key configured: {key_display} — source: {key_source}" if current_key else "⚠️ No API key configured"

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


def _run_job(job_id: str, source_path: Path, workdir: Path, anonymize: bool, autofix: bool, tailor: bool, jd_text: str, template_name: str, source_key: str | None, client_ip: str, started_at: float) -> None:
    try:
        def cb(status: str, progress: int) -> None:
            jobs.update(job_id, status=status, progress=progress)

        def dbg(text: str) -> None:
            jobs.update(job_id, debug=text)

        job_engine = QCVWebEngine(TEMPLATES_DIR)
        result_path = job_engine.process(
            source_path=source_path,
            output_dir=workdir,
            anonymize=anonymize,
            autofix=autofix,
            tailor=tailor,
            jd_text=jd_text,
            template_name=template_name,
            source_key=source_key,
            status_cb=cb,
            debug_cb=dbg,
        )

        job = jobs.get(job_id)
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

        jobs.update(job_id, status="Done", progress=100, result_path=str(result_path))
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


@app.post("/jobs")
async def create_job(
    request: Request,
    file: UploadFile = File(...),
    anonymize: bool = Form(False),
    autofix: bool = Form(False),
    tailor: bool = Form(False),
    jd_text: str = Form(""),
    template_name: str = Form(...),
):
    suffix = Path(file.filename or "upload.docx").suffix.lower()
    if suffix not in {".pdf", ".docx", ".png", ".jpg", ".jpeg"}:
        raise HTTPException(status_code=400, detail="Only PDF, DOCX, PNG, JPG, and JPEG are supported.")

    if tailor and not jd_text.strip():
        raise HTTPException(status_code=400, detail="Job description is required when tailoring is enabled.")

    if not template_name:
        raise HTTPException(status_code=400, detail="Template is required.")

    template_path = TEMPLATES_DIR / template_name
    if not template_path.exists():
        raise HTTPException(status_code=400, detail=f"Unknown template: {template_name}")

    job = jobs.create(
        file.filename or "uploaded_file",
        anonymize=anonymize,
        autofix=autofix,
        template_name=template_name,
    )
    workdir = make_temp_workspace()
    source_path = workdir / (file.filename or "uploaded_file")

    with source_path.open("wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)

    source_key = build_source_key(source_path)

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
        args=(job.job_id, source_path, workdir, anonymize, autofix, tailor, jd_text, template_name, source_key, client_ip, started_at),
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

    return {
        "job_id": job.job_id,
        "filename": job.filename,
        "status": job.status,
        "progress": job.progress,
        "error": job.error,
        "debug": getattr(job, "debug", ""),
        "ready": bool(job.result_path),
        "details": getattr(job, "details", None),
    }


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
