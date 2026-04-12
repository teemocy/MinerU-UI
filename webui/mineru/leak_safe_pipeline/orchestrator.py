from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from multiprocessing import get_context
from pathlib import Path
from queue import Empty
import json
import shutil
import threading
import tempfile
import uuid

from loguru import logger

from mineru.leak_safe_pipeline.splitter import PreparedDocument, SplitChunk, TOCSemanticSplitter
from mineru.leak_safe_pipeline.worker import WorkerTask, run_worker_once


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class OCRRequestConfig:
    api_url: str
    backend: str = "pipeline"
    parse_method: str = "auto"
    language: str = "ch"
    formula_enable: bool = True
    table_enable: bool = True
    server_url: str | None = None
    timeout_seconds: int = 7200


@dataclass
class ChunkExecutionRecord:
    chunk_id: str
    source_file: str
    chunk_file: str
    title: str
    status: str
    task_id: str | None = None
    output_dir: str | None = None
    error: str | None = None


@dataclass
class JobRecord:
    job_id: str
    status: str
    message: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    total_chunks: int = 0
    completed_chunks: int = 0
    failed_chunks: int = 0
    current_chunk_id: str | None = None
    current_source_file: str | None = None
    output_root: str | None = None
    archive_path: str | None = None
    notes: list[str] = field(default_factory=list)
    results: list[ChunkExecutionRecord] = field(default_factory=list)


class LeakSafeTaskManager:
    """
    Orchestrates OCR jobs with strict one-chunk-one-process isolation.

    Each chunk is executed inside a fresh Python process. After each chunk finishes,
    the worker process is joined and force-terminated if still alive.
    """

    def __init__(
        self,
        *,
        workspace_root: Path | str | None = None,
        cleanup_workspace_on_start: bool | None = None,
        splitter: TOCSemanticSplitter | None = None,
    ) -> None:
        use_default_workspace = workspace_root is None
        if workspace_root is None:
            workspace_root = Path(tempfile.gettempdir()) / "mineru-ocr-webui"
        if cleanup_workspace_on_start is None:
            cleanup_workspace_on_start = use_default_workspace

        self.workspace_root = Path(workspace_root).resolve()
        if cleanup_workspace_on_start and self.workspace_root.exists():
            shutil.rmtree(self.workspace_root, ignore_errors=True)
        self.workspace_root.mkdir(parents=True, exist_ok=True)
        self.splitter = splitter or TOCSemanticSplitter()

        self._lock = threading.Lock()
        self._jobs: dict[str, JobRecord] = {}

    def submit_job(
        self,
        *,
        inputs: list[Path | str],
        request: OCRRequestConfig,
        output_root: Path | str | None = None,
    ) -> str:
        resolved_inputs = [Path(path).expanduser().resolve() for path in inputs]
        if not resolved_inputs:
            raise ValueError("No input files were provided.")

        job_id = uuid.uuid4().hex[:12]
        job_root = (
            Path(output_root).expanduser().resolve() if output_root else self.workspace_root
        ) / job_id
        job_root.mkdir(parents=True, exist_ok=True)

        job_record = JobRecord(
            job_id=job_id,
            status="queued",
            message="Queued",
            created_at=_utc_now_iso(),
            output_root=str(job_root),
        )

        with self._lock:
            self._jobs[job_id] = job_record

        thread = threading.Thread(
            target=self._run_job,
            kwargs={
                "job_id": job_id,
                "inputs": resolved_inputs,
                "request": request,
                "job_root": job_root,
            },
            daemon=True,
        )
        thread.start()
        return job_id

    def get_job_snapshot(self, job_id: str) -> dict:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(f"Unknown job id: {job_id}")
            record = self._jobs[job_id]
            return {
                "job_id": record.job_id,
                "status": record.status,
                "message": record.message,
                "created_at": record.created_at,
                "started_at": record.started_at,
                "finished_at": record.finished_at,
                "total_chunks": record.total_chunks,
                "completed_chunks": record.completed_chunks,
                "failed_chunks": record.failed_chunks,
                "current_chunk_id": record.current_chunk_id,
                "current_source_file": record.current_source_file,
                "output_root": record.output_root,
                "archive_path": record.archive_path,
                "notes": list(record.notes),
                "results": [asdict(item) for item in record.results],
            }

    def _run_job(
        self,
        *,
        job_id: str,
        inputs: list[Path],
        request: OCRRequestConfig,
        job_root: Path,
    ) -> None:
        prepared_dir = job_root / "prepared"
        results_dir = job_root / "results"
        prepared_dir.mkdir(parents=True, exist_ok=True)
        results_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._update_job(
                job_id,
                status="preprocessing",
                message="Running TOC-based pre-processing...",
                started_at=_utc_now_iso(),
            )

            prepared_documents = self.splitter.prepare_many(inputs, prepared_dir)
            all_chunks = self._flatten_chunks(prepared_documents)
            notes = self._collect_notes(prepared_documents)

            self._update_job(
                job_id,
                status="running",
                message=f"Prepared {len(all_chunks)} chunk(s). Starting OCR.",
                total_chunks=len(all_chunks),
                notes=notes,
            )

            for chunk in all_chunks:
                self._update_job(
                    job_id,
                    current_chunk_id=chunk.chunk_id,
                    current_source_file=chunk.source_path.name,
                    message=f"Processing {chunk.chunk_id} in isolated worker process...",
                )

                chunk_output_dir = (
                    results_dir
                    / self._safe_path(chunk.source_path.stem)
                    / self._safe_path(chunk.chunk_id)
                )
                chunk_output_dir.mkdir(parents=True, exist_ok=True)
                staged_input_dir = chunk_output_dir / "_input"
                staged_input_path = staged_input_dir / chunk.chunk_path.name

                try:
                    staged_input_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(chunk.chunk_path, staged_input_path)

                    worker_task = WorkerTask(
                        chunk_id=chunk.chunk_id,
                        input_path=str(staged_input_path),
                        output_dir=str(chunk_output_dir),
                        api_url=request.api_url,
                        backend=request.backend,
                        parse_method=request.parse_method,
                        language=request.language,
                        formula_enable=request.formula_enable,
                        table_enable=request.table_enable,
                        server_url=request.server_url,
                        timeout_seconds=request.timeout_seconds,
                    )

                    worker_result = self._run_chunk_in_fresh_process(worker_task)
                    self._append_result(
                        job_id,
                        ChunkExecutionRecord(
                            chunk_id=chunk.chunk_id,
                            source_file=str(chunk.source_path),
                            chunk_file=str(chunk.chunk_path),
                            title=chunk.title,
                            status="completed",
                            task_id=worker_result.get("task_id"),
                            output_dir=worker_result.get("output_dir"),
                            error=None,
                        ),
                    )
                    self._increment_job_progress(job_id, completed_delta=1, failed_delta=0)
                except Exception as exc:
                    logger.exception("Chunk processing failed: {}", chunk.chunk_id)
                    self._append_result(
                        job_id,
                        ChunkExecutionRecord(
                            chunk_id=chunk.chunk_id,
                            source_file=str(chunk.source_path),
                            chunk_file=str(chunk.chunk_path),
                            title=chunk.title,
                            status="failed",
                            task_id=None,
                            output_dir=str(chunk_output_dir),
                            error=str(exc),
                        ),
                    )
                    self._increment_job_progress(job_id, completed_delta=0, failed_delta=1)
                finally:
                    shutil.rmtree(staged_input_dir, ignore_errors=True)

            archive_path = self._build_result_archive(job_id, job_root)
            snapshot = self.get_job_snapshot(job_id)
            if snapshot["completed_chunks"] == 0 and snapshot["failed_chunks"] > 0:
                self._update_job(
                    job_id,
                    status="failed",
                    message="All chunks failed. See chunk errors in the result table.",
                    finished_at=_utc_now_iso(),
                    current_chunk_id=None,
                    current_source_file=None,
                    archive_path=archive_path,
                )
                return

            if snapshot["failed_chunks"] > 0:
                final_message = (
                    f"Completed with partial failures: {snapshot['completed_chunks']} succeeded, "
                    f"{snapshot['failed_chunks']} failed."
                )
            else:
                final_message = f"Completed successfully: {snapshot['completed_chunks']} chunk(s)."

            self._update_job(
                job_id,
                status="completed",
                message=final_message,
                finished_at=_utc_now_iso(),
                current_chunk_id=None,
                current_source_file=None,
                archive_path=archive_path,
            )
        except Exception as exc:
            logger.exception("Job failed: {}", job_id)
            self._update_job(
                job_id,
                status="failed",
                message=f"Job failed: {exc}",
                finished_at=_utc_now_iso(),
                current_chunk_id=None,
                current_source_file=None,
            )

    def _run_chunk_in_fresh_process(self, worker_task: WorkerTask) -> dict:
        ctx = get_context("spawn")
        result_queue = ctx.Queue()
        process = ctx.Process(
            target=run_worker_once,
            args=(asdict(worker_task), result_queue),
        )

        process.start()
        timeout = worker_task.timeout_seconds + 30

        try:
            process.join(timeout)
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=5)
                raise TimeoutError(
                    f"Worker exceeded timeout for chunk {worker_task.chunk_id} and was killed."
                )

            try:
                result_payload = result_queue.get(timeout=2)
            except Empty:
                result_payload = None

            if result_payload is None:
                raise RuntimeError(
                    f"Worker exited without a result payload for chunk {worker_task.chunk_id}; "
                    f"exit_code={process.exitcode}"
                )

            if not result_payload.get("ok"):
                raise RuntimeError(result_payload.get("error") or "Worker returned failure")

            return result_payload
        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            process.close()
            result_queue.close()
            result_queue.join_thread()

    def _build_result_archive(self, job_id: str, job_root: Path) -> str:
        manifest_path = job_root / "results" / "manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot = self.get_job_snapshot(job_id)
        manifest_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        archive_base = job_root / "results_bundle"
        archive_path = shutil.make_archive(
            base_name=str(archive_base),
            format="zip",
            root_dir=str(job_root),
            base_dir="results",
        )
        return archive_path

    def _flatten_chunks(self, prepared_documents: list[PreparedDocument]) -> list[SplitChunk]:
        chunks: list[SplitChunk] = []
        for document in prepared_documents:
            chunks.extend(document.chunks)
        return chunks

    def _collect_notes(self, prepared_documents: list[PreparedDocument]) -> list[str]:
        notes: list[str] = []
        for document in prepared_documents:
            for note in document.notes:
                notes.append(f"{document.source_path.name}: {note}")
        return notes

    def _append_result(self, job_id: str, item: ChunkExecutionRecord) -> None:
        with self._lock:
            self._jobs[job_id].results.append(item)

    def _increment_job_progress(self, job_id: str, completed_delta: int, failed_delta: int) -> None:
        with self._lock:
            record = self._jobs[job_id]
            record.completed_chunks += completed_delta
            record.failed_chunks += failed_delta

    def _update_job(self, job_id: str, **kwargs) -> None:
        with self._lock:
            record = self._jobs[job_id]
            for key, value in kwargs.items():
                setattr(record, key, value)

    def _safe_path(self, value: str) -> str:
        cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)
        cleaned = cleaned.strip("._")
        return cleaned or "item"
