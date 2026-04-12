from __future__ import annotations

from pathlib import Path
import tempfile

import click

from mineru.leak_safe_pipeline.webui import DEFAULT_VLM_SERVER_URL, launch


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
    "--output-root",
    default=str(Path(tempfile.gettempdir()) / "mineru-ocr-webui"),
    show_default=True,
    help="Root directory for job artifacts",
)
def main(
    host: str,
    port: int,
    api_url: str,
    server_url: str,
    output_root: str,
) -> None:
    launch(
        host=host,
        port=port,
        default_api_url=api_url,
        default_server_url=server_url,
        default_output_root=output_root,
    )


if __name__ == "__main__":
    main()
