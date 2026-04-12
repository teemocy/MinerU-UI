from __future__ import annotations

from pathlib import Path
import shutil
import socket
import tempfile
import threading
import time
from urllib.parse import urlsplit, urlunsplit

import gradio as gr

from mineru.leak_safe_pipeline.orchestrator import (
    LeakSafeTaskManager,
    OCRRequestConfig,
)


SUPPORTED_SUFFIXES = {".pdf", ".docx"}
TERMINAL_STATUSES = {"completed", "failed"}
DEFAULT_VLM_SERVER_URL = "http://mineru-openai-server:30000"
TEMP_WORKSPACE_ROOT = Path(tempfile.gettempdir()).resolve() / "mineru-ocr-webui"
TEMP_DOWNLOAD_ROOT = Path(tempfile.gettempdir()).resolve() / "mineru-ocr-webui-downloads"
BACKEND_CHOICES = [
    "pipeline",
    "vlm-auto-engine",
    "vlm-http-client",
    "hybrid-auto-engine",
    "hybrid-http-client",
]
LANGUAGE_CHOICES = [
    "ch (Chinese, English, Chinese Traditional)",
    "ch_lite (Chinese, English, Chinese Traditional, Japanese)",
    "ch_server (Chinese, English, Chinese Traditional, Japanese)",
    "en (English)",
    "korean (Korean, English)",
    "japan (Chinese, English, Chinese Traditional, Japanese)",
    "chinese_cht (Chinese, English, Chinese Traditional, Japanese)",
    "ta (Tamil, English)",
    "te (Telugu, English)",
    "ka (Kannada)",
    "el (Greek, English)",
    "th (Thai, English)",
    "latin (French, German, Afrikaans, Italian, Spanish, Bosnian, Portuguese, Czech, Welsh, Danish, Estonian, Irish, Croatian, Uzbek, Hungarian, Serbian (Latin), Indonesian, Occitan, Icelandic, Lithuanian, Maori, Malay, Dutch, Norwegian, Polish, Slovak, Slovenian, Albanian, Swedish, Swahili, Tagalog, Turkish, Latin, Azerbaijani, Kurdish, Latvian, Maltese, Pali, Romanian, Vietnamese, Finnish, Basque, Galician, Luxembourgish, Romansh, Catalan, Quechua)",
    "arabic (Arabic, Persian, Uyghur, Urdu, Pashto, Kurdish, Sindhi, Balochi, English)",
    "east_slavic (Russian, Belarusian, Ukrainian, English)",
    "cyrillic (Russian, Belarusian, Ukrainian, Serbian (Cyrillic), Bulgarian, Mongolian, Abkhazian, Adyghe, Kabardian, Avar, Dargin, Ingush, Chechen, Lak, Lezgin, Tabasaran, Kazakh, Kyrgyz, Tajik, Macedonian, Tatar, Chuvash, Bashkir, Malian, Moldovan, Udmurt, Komi, Ossetian, Buryat, Kalmyk, Tuvan, Sakha, Karakalpak, English)",
    "devanagari (Hindi, Marathi, Nepali, Bihari, Maithili, Angika, Bhojpuri, Magahi, Santali, Newari, Konkani, Sanskrit, Haryanvi, English)",
]

_MANAGER: LeakSafeTaskManager | None = None
_MANAGER_LOCK = threading.Lock()


def _get_manager() -> LeakSafeTaskManager:
    global _MANAGER

    if _MANAGER is not None:
        return _MANAGER

    with _MANAGER_LOCK:
        if _MANAGER is None:
            _MANAGER = LeakSafeTaskManager(
                workspace_root=TEMP_WORKSPACE_ROOT,
                cleanup_workspace_on_start=False,
            )
    return _MANAGER


def _prepare_launch_workspace(default_output_root: str | Path) -> None:
    shutil.rmtree(TEMP_DOWNLOAD_ROOT, ignore_errors=True)

    resolved_output_root = Path(default_output_root).expanduser().resolve()
    if resolved_output_root == TEMP_WORKSPACE_ROOT:
        shutil.rmtree(TEMP_WORKSPACE_ROOT, ignore_errors=True)


def _coerce_uploaded_paths(items) -> list[Path]:
    if items is None:
        return []

    if not isinstance(items, list):
        items = [items]

    paths: list[Path] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, str):
            candidate = item
        else:
            candidate = getattr(item, "name", None)
        if not candidate:
            continue
        path = Path(candidate).expanduser().resolve()
        if path.exists() and path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES:
            paths.append(path)
    return paths


def _discover_directory_inputs(directory_path: str | None) -> list[Path]:
    if directory_path is None:
        return []
    directory_path = directory_path.strip()
    if not directory_path:
        return []

    folder = Path(directory_path).expanduser().resolve()
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Directory does not exist: {folder}")

    collected = [
        path
        for path in folder.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES
    ]
    return sorted(collected)


def _collect_inputs(
    uploaded_files,
    directory_path: str | None,
) -> list[Path]:
    collected: list[Path] = []
    collected.extend(_coerce_uploaded_paths(uploaded_files))
    collected.extend(_discover_directory_inputs(directory_path))

    unique: list[Path] = []
    seen = set()
    for path in collected:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _format_status(snapshot: dict) -> str:
    status = snapshot["status"]
    completed = snapshot["completed_chunks"]
    failed = snapshot["failed_chunks"]
    total = snapshot["total_chunks"]

    lines = [
        f"**Job ID:** `{snapshot['job_id']}`",
        f"**Status:** `{status}`",
        f"**Progress:** `{completed + failed}/{max(total, 1)}` chunk(s)",
        f"**Message:** {snapshot['message']}",
    ]
    if snapshot.get("current_chunk_id"):
        lines.append(f"**Current Chunk:** `{snapshot['current_chunk_id']}`")
    return "\n\n".join(lines)


def _snapshot_rows(snapshot: dict) -> list[list[str]]:
    rows: list[list[str]] = []
    for item in snapshot["results"]:
        rows.append(
            [
                item.get("chunk_id") or "",
                Path(item.get("source_file") or "").name,
                item.get("title") or "",
                item.get("status") or "",
                item.get("task_id") or "",
                (item.get("error") or "")[:400],
            ]
        )
    return rows


def _notes_text(snapshot: dict) -> str:
    notes = snapshot.get("notes") or []
    if not notes:
        return "No preprocessing notes."
    return "\n".join(f"- {item}" for item in notes)


def _normalize_language_choice(language: str | None) -> str:
    if not language:
        return "ch"
    if "(" in language and ")" in language:
        return language.split("(", 1)[0].strip()
    return language.strip() or "ch"


def _backend_supports_language_choice(backend: str | None) -> bool:
    if not backend:
        return False
    return backend == "pipeline" or backend.startswith("hybrid-")


def _language_visibility_update(backend: str):
    return gr.update(visible=_backend_supports_language_choice(backend))


def _normalize_api_url(api_url: str | None) -> str:
    cleaned = (api_url or "").strip()
    if not cleaned:
        return ""

    parts = urlsplit(cleaned)
    if parts.hostname != "host.docker.internal":
        return cleaned

    try:
        ipv4_records = socket.getaddrinfo(
            parts.hostname,
            None,
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror:
        return cleaned

    if not ipv4_records:
        return cleaned

    resolved_host = ipv4_records[0][4][0]
    if not resolved_host:
        return cleaned

    auth = ""
    if parts.username is not None:
        auth = parts.username
        if parts.password is not None:
            auth = f"{auth}:{parts.password}"
        auth = f"{auth}@"

    netloc = f"{auth}{resolved_host}"
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _prepare_downloadable_archive(archive_path: str | None, job_id: str) -> str | None:
    if not archive_path:
        return None

    source = Path(archive_path).resolve()
    if not source.exists() or not source.is_file():
        return None

    TEMP_DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    target = TEMP_DOWNLOAD_ROOT / f"{job_id}-{source.name}"

    if (
        not target.exists()
        or target.stat().st_size != source.stat().st_size
        or target.stat().st_mtime_ns < source.stat().st_mtime_ns
    ):
        shutil.copy2(source, target)

    return str(target)


def _stream_job(
    uploaded_files,
    directory_path,
    api_url,
    backend,
    parse_method,
    language,
    formula_enable,
    table_enable,
    server_url,
    output_root,
    timeout_seconds,
):
    try:
        inputs = _collect_inputs(uploaded_files, directory_path)
    except Exception as exc:
        yield (
            "",
            f"**Input Error:** {exc}",
            0.0,
            [],
            "",
            None,
        )
        return

    if not inputs:
        yield (
            "",
            "**Input Error:** Please upload at least one `.pdf` or `.docx`, or provide a directory path.",
            0.0,
            [],
            "",
            None,
        )
        return

    request = OCRRequestConfig(
        api_url=_normalize_api_url(api_url),
        backend=backend,
        parse_method=parse_method,
        language=_normalize_language_choice(language),
        formula_enable=bool(formula_enable),
        table_enable=bool(table_enable),
        server_url=server_url.strip() or None,
        timeout_seconds=int(timeout_seconds),
    )

    manager = _get_manager()
    job_id = manager.submit_job(
        inputs=inputs,
        request=request,
        output_root=output_root.strip() or None,
    )

    while True:
        snapshot = manager.get_job_snapshot(job_id)
        total = max(snapshot["total_chunks"], 1)
        progress = (snapshot["completed_chunks"] + snapshot["failed_chunks"]) / total

        archive_file = snapshot.get("archive_path")
        if archive_file and not Path(archive_file).exists():
            archive_file = None
        archive_file = _prepare_downloadable_archive(archive_file, job_id)

        yield (
            job_id,
            _format_status(snapshot),
            progress,
            _snapshot_rows(snapshot),
            _notes_text(snapshot),
            archive_file,
        )

        if snapshot["status"] in TERMINAL_STATUSES:
            return
        time.sleep(1.0)


def build_app(
    *,
    default_api_url: str = "http://127.0.0.1:8000",
    default_server_url: str = DEFAULT_VLM_SERVER_URL,
    default_output_root: str = str(TEMP_WORKSPACE_ROOT),
) -> gr.Blocks:
    with gr.Blocks(title="MinerU OCR WebUI") as app:
        gr.Markdown(
            "# MinerU Leak-Safe OCR Pipeline\n"
            "One-doc-one-process execution with TOC/bookmark semantic splitting (<300 pages per request)."
        )

        with gr.Row():
            uploaded_files = gr.File(
                label="Documents",
                file_types=[".pdf", ".docx"],
                file_count="multiple",
                type="filepath",
            )

        directory_path = gr.Textbox(
            label="Batch Directory Path (server-side)",
            placeholder="/path/to/folder",
            info="Optional: recursively loads .pdf and .docx files from this folder.",
        )

        with gr.Accordion("Execution Settings", open=False):
            api_url = gr.Textbox(label="MinerU API URL", value=default_api_url)
            backend = gr.Dropdown(
                label="Backend",
                choices=BACKEND_CHOICES,
                value="vlm-http-client",
            )
            parse_method = gr.Radio(
                label="Parse Method",
                choices=["auto", "txt", "ocr"],
                value="auto",
            )
            with gr.Row(visible=False) as language_row:
                language = gr.Dropdown(
                    label="OCR Language",
                    choices=LANGUAGE_CHOICES,
                    value="ch (Chinese, English, Chinese Traditional)",
                )
            formula_enable = gr.Checkbox(label="Formula Enable", value=True)
            table_enable = gr.Checkbox(label="Table Enable", value=True)
            server_url = gr.Textbox(
                label="VLM Server URL (optional)",
                value=default_server_url,
                placeholder="http://127.0.0.1:30000",
            )
            output_root = gr.Textbox(
                label="Job Output Root",
                value=default_output_root,
                info="Defaults to a temporary directory and is cleared when the app restarts.",
            )
            timeout_seconds = gr.Number(
                label="Per Chunk Timeout (seconds)",
                value=7200,
                precision=0,
            )

        run_button = gr.Button("Start OCR Job", variant="primary")

        with gr.Row():
            job_id = gr.Textbox(label="Job ID", interactive=False)
            progress = gr.Slider(
                label="Progress",
                minimum=0.0,
                maximum=1.0,
                value=0.0,
                step=0.01,
                interactive=False,
            )

        status = gr.Markdown("Idle")
        notes = gr.Textbox(label="Preprocessing Notes", interactive=False, lines=4)
        results = gr.Dataframe(
            headers=[
                "chunk_id",
                "source_file",
                "title",
                "status",
                "task_id",
                "error",
            ],
            datatype=["str", "str", "str", "str", "str", "str"],
            value=[],
            wrap=True,
            interactive=False,
        )
        archive = gr.File(label="Result Bundle (.zip)", interactive=False)

        backend.change(
            fn=_language_visibility_update,
            inputs=[backend],
            outputs=[language_row],
        )

        run_button.click(
            fn=_stream_job,
            inputs=[
                uploaded_files,
                directory_path,
                api_url,
                backend,
                parse_method,
                language,
                formula_enable,
                table_enable,
                server_url,
                output_root,
                timeout_seconds,
            ],
            outputs=[job_id, status, progress, results, notes, archive],
        )

    return app


def launch(
    *,
    host: str = "127.0.0.1",
    port: int = 7861,
    default_api_url: str = "http://127.0.0.1:8000",
    default_server_url: str = DEFAULT_VLM_SERVER_URL,
    default_output_root: str = str(TEMP_WORKSPACE_ROOT),
) -> None:
    _prepare_launch_workspace(default_output_root)
    app = build_app(
        default_api_url=default_api_url,
        default_server_url=default_server_url,
        default_output_root=default_output_root,
    )
    app.launch(server_name=host, server_port=port)
