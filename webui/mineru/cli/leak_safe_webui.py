from __future__ import annotations

from pathlib import Path
import tempfile

import click

from mineru.leak_safe_pipeline.webui import DEFAULT_VLM_SERVER_URL, launch
from mineru.leak_safe_pipeline.splitter import MAX_PAGES_PER_REQUEST


@click.command(help="Launch MinerU OCR WebUI")
@click.option("--host", default="127.0.0.1", show_default=True, help="WebUI host")
@click.option("--port", default=7861, type=int, show_default=True, help="WebUI port")
@click.option(
    "--api-url",
    default="http://127.0.0.1:8000",
    show_default=True,
    help="MinerU API base URL",
)
@click.option(
    "--server-url",
    default=DEFAULT_VLM_SERVER_URL,
    show_default=True,
    help=(
        "Default OpenAI-compatible VLM server URL shown in the WebUI. "
        "Use http://mineru-openai-server:30000 for the Docker-compose stack."
    ),
)
@click.option(
    "--default-backend",
    default="vlm-http-client",
    show_default=True,
    help="Backend selected by default in the WebUI.",
)
@click.option(
    "--output-root",
    default=str(Path(tempfile.gettempdir()) / "mineru-ocr-webui"),
    show_default=True,
    help="Root directory for job artifacts",
)
@click.option(
    "--default-max-pages-per-chunk",
    default=MAX_PAGES_PER_REQUEST,
    type=click.IntRange(min=1),
    show_default=True,
    help="Default value for the WebUI Max Pages Per Chunk setting.",
)
def main(
    host: str,
    port: int,
    api_url: str,
    server_url: str,
    default_backend: str,
    output_root: str,
    default_max_pages_per_chunk: int,
) -> None:
    launch(
        host=host,
        port=port,
        default_api_url=api_url,
        default_server_url=server_url,
        default_backend=default_backend,
        default_output_root=output_root,
        default_max_pages_per_request=default_max_pages_per_chunk,
    )


if __name__ == "__main__":
    main()
