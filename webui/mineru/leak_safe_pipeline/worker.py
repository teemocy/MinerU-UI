from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import json
import time
import traceback

import httpx
from loguru import logger

from mineru.cli import api_client


TRANSIENT_BACKEND_ERROR_PATTERNS = ("already borrowed",)
TRANSIENT_BACKEND_RETRY_LIMIT = 2
TRANSIENT_BACKEND_RETRY_BACKOFF_SECONDS = 2.0
TRANSIENT_BACKEND_RETRY_BACKOFF_CAP_SECONDS = 8.0


@dataclass(frozen=True)
class WorkerTask:
    chunk_id: str
    input_path: str
    output_dir: str
    api_url: str
    backend: str
    parse_method: str
    language: str
    formula_enable: bool
    table_enable: bool
    server_url: str | None
    timeout_seconds: int = 7200


@dataclass(frozen=True)
class WorkerResult:
    ok: bool
    chunk_id: str
    task_id: str | None
    output_dir: str
    result_zip: str | None
    error: str | None


def run_worker_once(task_payload: dict, result_queue) -> None:
    """Multiprocessing entrypoint. Handles exactly one chunk and exits."""

    task = WorkerTask(**task_payload)
    chunk_path = Path(task.input_path).resolve()
    output_dir = Path(task.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    task_id: str | None = None
    result_zip_path: Path | None = None
    total_attempts = 1 + TRANSIENT_BACKEND_RETRY_LIMIT
    deadline = time.monotonic() + task.timeout_seconds

    for attempt in range(1, total_attempts + 1):
        task_id = None
        result_zip_path = None
        try:
            task_id, result_zip_path = _run_worker_task_attempt(
                task=task,
                chunk_path=chunk_path,
                output_dir=output_dir,
                timeout_seconds=_remaining_time_seconds(deadline),
            )
            api_client.safe_extract_zip(result_zip_path, output_dir)
            result = WorkerResult(
                ok=True,
                chunk_id=task.chunk_id,
                task_id=task_id,
                output_dir=str(output_dir),
                result_zip=str(result_zip_path),
                error=None,
            )
            result_queue.put(asdict(result))
            return
        except Exception as exc:
            error_text = f"{exc}\n{traceback.format_exc()}"
            should_retry = _should_retry_transient_backend_failure(
                error_text=error_text,
                attempt=attempt,
                total_attempts=total_attempts,
                deadline=deadline,
            )
            if should_retry:
                backoff_seconds = min(
                    _compute_transient_retry_backoff_seconds(attempt),
                    _remaining_time_seconds(deadline, minimum=0),
                )
                logger.warning(
                    "Transient VLM backend overload for {} on attempt {}/{}. "
                    "Retrying in {:.1f}s. task_id={}",
                    chunk_path.name,
                    attempt,
                    total_attempts,
                    backoff_seconds,
                    task_id,
                )
                if backoff_seconds > 0:
                    time.sleep(backoff_seconds)
                continue

            if attempt > 1 and _is_transient_backend_failure(error_text):
                error_text = (
                    f"Transient backend overload persisted after {attempt} attempt(s) "
                    f"for {chunk_path.name}.\nLast error:\n{error_text}"
                )
            result = WorkerResult(
                ok=False,
                chunk_id=task.chunk_id,
                task_id=task_id,
                output_dir=str(output_dir),
                result_zip=str(result_zip_path) if result_zip_path else None,
                error=error_text,
            )
            result_queue.put(asdict(result))
            return
        finally:
            if result_zip_path is not None:
                result_zip_path.unlink(missing_ok=True)


def _run_worker_task_attempt(
    task: WorkerTask,
    chunk_path: Path,
    output_dir: Path,
    timeout_seconds: float,
) -> tuple[str, Path]:
    if not chunk_path.exists() or not chunk_path.is_file():
        raise FileNotFoundError(f"Chunk not found: {chunk_path}")

    if timeout_seconds <= 0:
        raise TimeoutError(
            f"Timed out waiting for MinerU task completion for {chunk_path.name}."
        )

    base_url = api_client.normalize_base_url(task.api_url)
    form_data = api_client.build_parse_request_form_data(
        lang_list=[task.language],
        backend=task.backend,
        parse_method=task.parse_method,
        formula_enable=task.formula_enable,
        table_enable=task.table_enable,
        server_url=task.server_url,
        start_page_id=0,
        end_page_id=None,
        return_md=True,
        return_middle_json=True,
        return_model_output=False,
        return_content_list=True,
        return_images=True,
        response_format_zip=True,
        return_original_file=True,
    )
    upload_assets = [
        api_client.UploadAsset(
            path=chunk_path,
            upload_name=chunk_path.name,
        )
    ]

    with httpx.Client(timeout=api_client.build_http_timeout(), follow_redirects=True) as client:
        health_response = client.get(f"{base_url}/health")
        if health_response.status_code != 200:
            raise RuntimeError(
                f"MinerU API health check failed: {health_response.status_code} {health_response.text}"
            )

        submit_response = api_client.submit_parse_task_sync(
            base_url=base_url,
            upload_assets=upload_assets,
            form_data=form_data,
        )

        _wait_for_terminal_status(
            client=client,
            status_url=submit_response.status_url,
            chunk_label=chunk_path.name,
            timeout_seconds=timeout_seconds,
        )

        result_zip_path = _download_result_zip(
            client=client,
            result_url=submit_response.result_url,
            chunk_label=chunk_path.name,
            output_dir=output_dir,
        )
    return submit_response.task_id, result_zip_path


def _remaining_time_seconds(deadline: float, minimum: float = 0.0) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Timed out before the next worker attempt could start.")
    return remaining if remaining >= minimum else minimum


def _is_transient_backend_failure(error_text: str) -> bool:
    normalized_error = error_text.lower()
    return any(pattern in normalized_error for pattern in TRANSIENT_BACKEND_ERROR_PATTERNS)


def _compute_transient_retry_backoff_seconds(attempt: int) -> float:
    return min(
        TRANSIENT_BACKEND_RETRY_BACKOFF_CAP_SECONDS,
        TRANSIENT_BACKEND_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)),
    )


def _should_retry_transient_backend_failure(
    *,
    error_text: str,
    attempt: int,
    total_attempts: int,
    deadline: float,
) -> bool:
    if attempt >= total_attempts:
        return False
    if not _is_transient_backend_failure(error_text):
        return False
    return deadline - time.monotonic() > 0


def _wait_for_terminal_status(
    client: httpx.Client,
    status_url: str,
    chunk_label: str,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        response = client.get(status_url)
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to query task status for {chunk_label}: {response.status_code} {response.text}"
            )
        payload = response.json()
        status = payload.get("status")
        if status in {"pending", "processing"}:
            time.sleep(api_client.TASK_STATUS_POLL_INTERVAL_SECONDS)
            continue
        if status == "completed":
            return
        raise RuntimeError(
            f"MinerU task failed for {chunk_label}: {json.dumps(payload, ensure_ascii=False)}"
        )

    raise TimeoutError(
        f"Timed out waiting for MinerU task completion for {chunk_label}."
    )


def _download_result_zip(
    client: httpx.Client,
    result_url: str,
    chunk_label: str,
    output_dir: Path,
) -> Path:
    response = client.get(result_url)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to download result ZIP for {chunk_label}: {response.status_code} {response.text}"
        )

    content_type = response.headers.get("content-type", "")
    if "application/zip" not in content_type:
        raise RuntimeError(
            f"Expected ZIP result for {chunk_label}; received content-type={content_type or 'unknown'}"
        )

    output_zip = output_dir / "result.zip"
    output_zip.write_bytes(response.content)
    return output_zip
