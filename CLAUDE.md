# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**webQCVT** is a web service that converts and **tailors** CVs from PDF, DOCX, or image formats into standardized Quantori Word document templates using Google Gemini AI. It is the "Tailor" variant of webQCV — same core engine but with JD-based tailoring, relevance checking, and keyword refinement.

**Related projects (same machine, separate repos):**
- `Q-CV` (desktop) — Flet desktop app, shares `cv_engine.py` prompts
- `webQCV` — simpler web converter without tailoring (DO NOT modify without explicit request)

## Running the Application

```bash
source .venv/bin/activate
uvicorn app:app --host 0.0.0.0 --port 8000

# With auto-reload for development
uvicorn app:app --reload --port 8000
```

The app serves the frontend at `/` and the admin dashboard at `/admin/usage`.

## Environment / Configuration

- **API key**: Gemini API key loaded from `~/.quantoricv_settings.json` under key `"api_key"`, or env `GEMINI_API_KEY`, or `.api_key` file
- **Master prompts**: Override prompts via `~/.master_prompts.json`
- **Templates**: `.docx` template files in `templates/`, discovered automatically
- **Cache**: Base JSON cached in `_cache/base_json/{sha256}.base.json` — safe to delete to force re-extraction
- **Usage log**: Appended to `usage_log.jsonl`
- **DATA_DIR**: Env variable to redirect `_store/`, `_cache/`, `usage_log.jsonl` to persistent disk (used on Render)

## Architecture

```
index.html              Vanilla JS single-page frontend; polls /jobs/{id} every 1.5s
app.py                  FastAPI endpoints; spawns background threads for jobs
converter_engine.py     Job orchestration: parse → check → tailor → anonymize → render
cv_engine.py            Core logic: LLM schema, prompts, sanitization, anonymization, DOCX generation
source_baseline_extractor.py  Raw text extraction from PDF/DOCX inputs
templates/              Quantori .docx template files (docxtpl-rendered)
_cache/                 File-based cache keyed by SHA256 of source content
_store/                 Persistent CV store (JSON files keyed by SHA256 of source)
```

### Processing Pipeline

1. `POST /jobs` — saves uploaded file, enqueues job, returns `job_id`
2. Background thread (`_run_job`, throttled by `_JOB_SEMAPHORE` max 5 concurrent) runs the pipeline:
   - Hash source → check `_cache/` for previously extracted base JSON
   - If cache miss: Gemini extract CV into `CV_JSON_SCHEMA`
   - Optional: autofix pass
   - If tailor enabled:
     - Gap analysis (`_analyze_gap`) → LLM evaluates CV-JD fit → `match_percentage`, strengths/weaknesses, skills table
     - `gap_ready_cb` saves result to store (`_save_store_gap`), updates match_pct in meta
     - Pipeline pauses (`pause_event.wait`) until user reviews and clicks "Generate Tailored CV"
     - `_apply_tailor()` (LLM rewrite with focus skills)
   - Optional: anonymization (`smart_anonymize_data`)
   - Build content details + JD keyword report
   - Render DOCX via `docxtpl`
   - Auto-save to `_store/` with tailor session data
3. Frontend polls `GET /jobs/{job_id}` for `{status, progress, ready, details, gap_analysis}`
4. `GET /jobs/{job_id}/download` returns the generated DOCX

### Tailoring Features

- **Relevance check**: Deterministic keyword overlap (`_check_relevance`). Dual ratio `max(jd_ratio, cv_ratio)`. LOW (<5%) blocks tailoring unless force_tailor=true.
- **JD validation**: `validate_jd()` rejects empty/short/non-JD text (20 char, 5 word thresholds, `_JD_MARKERS` set).
- **Keyword report**: `_compute_jd_keyword_report()` compares JD vs tailored CV, returns matched/missing/added lists with match percentage. Shown in UI modal.
- **Refine (2nd pass)**: `POST /jobs/{id}/refine` — surgical LLM pass that weaves missing JD keywords into already-tailored CV. Limited to one refine per job. Uses `prompt_refine`.
- **Title cleanup**: Tech terms rescued from LinkedIn parenthesized titles into `skills["Title Specialties"]`.
- **Universal skills**: Tailor prompt rule 8 (MANDATORY) ensures data structures, algorithms, code review, testing, agile are always added to skills/highlights.
- **Markdown cleanup**: `_strip_markdown_bold()` removes `**bold**` markers from LLM output before DOCX rendering.

### Key Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Serves index.html |
| POST | `/jobs` | Create conversion job (file + options) |
| GET | `/jobs/{id}` | Poll job status, progress, details |
| GET | `/jobs/{id}/download` | Download generated DOCX |
| GET | `/jobs/{id}/cv_json` | Get extracted base CV JSON |
| POST | `/jobs/{id}/continue` | Resume paused job after gap analysis (sends focus_skills) |
| POST | `/jobs/{id}/refine` | Trigger keyword refinement pass |
| GET | `/store` | List all stored CVs (_meta only) |
| GET | `/store/{id}` | Get full stored CV with tailor/gap sessions |
| DELETE | `/store/{id}` | Delete stored CV |
| PATCH | `/store/{id}/meta` | Update editable meta field (e.g. comments) |
| POST | `/jobs/{id}/cancel` | Cancel a running job |
| POST | `/store/batch` | Batch actions: generate, delete, anonymize, analyze |
| GET | `/stats` | Server stats (active jobs, today count, uptime) |
| GET | `/templates` | List available templates |
| GET | `/admin/prompts` | Get prompt overrides and defaults |
| PUT | `/admin/prompts/{key}` | Save prompt override |
| DELETE | `/admin/prompts/{key}` | Reset prompt to default |
| GET | `/admin/usage` | Usage dashboard |

### Key Data

- **`CV_JSON_SCHEMA`** (in `cv_engine.py`) — canonical schema for LLM extraction
- **`DEFAULT_PROMPTS`** (in `cv_engine.py`) — all LLM prompts including `prompt_tailor`, `prompt_refine`, `prompt_anonymize`
- **Job state** — `InMemoryJobStore` (thread-safe with `_lock`) with `JobState` dataclass; after tailor, jobs store `_tailored_json`, `_jd_text`, `_gap_analysis`, `_focus_skills` for refine/store reuse
- **CV Store** — `_store/{sha256}.json` files with `_meta` (name, role, company, date, analyzed, tailored, match_pct, file), `_gap_session`, `_tailor_session`. Protected by `_STORE_LOCK` for concurrent writes. Store IDs validated as hex-only to prevent path traversal. Re-analysis resets `tailored=False` and removes `_tailor_session`.

### LLM Integration

- Model: `gemini-2.0-flash` (`choose_model_name()` in `converter_engine.py`)
- SDK: `from google import genai` — `genai.Client(api_key=...)` per call
- Images/PDFs uploaded via `client.files.upload()` with state polling until `ACTIVE`
- Retry logic for 429/quota errors in `_retry_on_rate_limit()` with delays [5,5,5,10,10]

### Frontend (index.html)

- Single-file vanilla JS, no build step
- **Four tabs**: Convert, History, Logs, Prompt (horizontal, browser-style)
- Stats bar in header (polls `/stats` every 10s)

**Convert tab:**
- File upload with template/anonymize/tailor options
- JD textarea appears when "Tailor to JD" is checked
- Two-step tailor flow: Analyze (gap analysis + Fit Report) → Generate Tailored CV
- Fit Report shows match %, strengths/weaknesses, skills assessment table with checkboxes
- Auto-checked universal skills (data structures, algorithms, code review, testing, agile)
- After DOCX generation: keyword match report modal (matched/missing/added)
- Refine button (one-shot keyword weaving)
- Low relevance → confirm dialog → force_tailor resubmit
- CV JSON editor panel (collapsible, tabbed by section)
- Skip redundant gap analysis when CV already analyzed with same JD (via `skip_gap` + `preloadedFitJson`)

**History tab (formerly Batch):**
- Grid of stored CVs with columns: Name, Match%, Role, Company, File, Comments, Date
- Columns: draggable reorder (saved to localStorage as `batchColOrder`), pixel-based resizable widths (`batchColPx`), visibility toggle via Cols menu, dblclick auto-fit
- Match% colored by value: green (≥70%), yellow (≥40%), red (<40%)
- Import CVs button (file picker, dedup by candidate name)
- Batch Analyze: mass gap analysis with shared JD, sequential processing, Stop button, skips already-analyzed CVs with same JD
- Batch Generate/Delete with shift+click range select
- Auto-download DOCXs as they complete during batch generate
- Click row → opens CV in Convert tab (with tailor session if available)
- Search with clear button, sort by any column (persisted in localStorage as `batchSort`)
- Row highlighting during batch processing
- Checkbox state preserved across re-renders and sort changes

**Logs tab:** Debug log with timestamps, auto-scroll, clear button
**Prompt tab:** Edit/save/reset LLM prompts (tailor, refine, etc.)

**Key frontend state variables:**
- `activeJobId` — current polling job
- `gapAnalysisReady` / `hasGapData` — gap analysis flow control (must stay in sync)
- `preloadedFitJson` — loaded gap data from store (used for skip_gap)
- `currentStoreId` — store ID of CV loaded from History (passed as `store_id` in form)
- `optionsChangedSinceJob` — tracks template/option changes after gap analysis
- `batchCancelled` — flag to stop batch orchestrator and frontend polling
- `batchRemainingJobs` — Map of active batch job IDs for polling/cancellation

### Caching

Source files SHA256-hashed; base JSON reused across re-submissions with different options. Tailor/anonymize applied on top of cached base JSON.

### Batch Analyze

- `_run_batch_analyze()` — lightweight gap-only analysis (no DOCX generation)
- `_batch_analyze_orchestrator()` — single-thread sequential executor, processes CVs in current sort order
- Cancel-aware: checks `_cancelled` flag between jobs; semaphore wait uses 1s timeout loop
- Skips CVs already analyzed with the same JD text
- Frontend polls active jobs via `pollBatchJobs()`, updates rows in real-time via `updateBatchRow()`

### Syncing with Desktop (Q-CV)

Prompts in `cv_engine.py` are shared between desktop and web. When improving prompts:
1. Edit in webQCVT first (easier to test)
2. Copy prompt changes to `Q-CV/cv_engine.py`
3. Do NOT sync web-specific code (endpoints, UI, converter_engine) to desktop

### Thread Safety

- `_JOB_SEMAPHORE` (Semaphore(5)) — throttles concurrent LLM jobs
- `_SemaphorePause` — wrapper that releases semaphore during `pause_event.wait()` to avoid starvation
- `_STORE_LOCK` (threading.Lock) — serializes all store file writes (`_save_to_store`, `_save_store_gap`, `_update_store_tailor`) and `_store_cache` access
- `_store_cache` — in-memory list of `_meta` dicts, thread-safe via `_STORE_LOCK`
- `InMemoryJobStore._lock` — protects job dict operations
- Store IDs validated via `_validate_store_id()` (hex-only) to prevent path traversal
- Store deduplication: `_find_store_by_name()` reuses existing store ID when same person uploaded from different file

### Limitations

- **Single-instance only**: job state in-memory, no multi-server scaling
- **No authentication**: public API with IP logging only
- **No database**: job queue in-memory; usage log is flat JSONL
- **Persistent store**: CV store in `_store/` survives restarts; generated DOCX files are ephemeral
