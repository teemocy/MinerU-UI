# WebUI Overlay

This directory contains the custom leak-safe OCR WebUI sources.

Current layout:

- `mineru/cli/leak_safe_webui.py`
- `mineru/leak_safe_pipeline/__init__.py`
- `mineru/leak_safe_pipeline/splitter.py`
- `mineru/leak_safe_pipeline/worker.py`
- `mineru/leak_safe_pipeline/orchestrator.py`
- `mineru/leak_safe_pipeline/webui.py`

These files are copied over the upstream submodule during image builds or when running:

```bash
./scripts/apply_customizations.sh third_party/MinerU webui
```

The WebUI is kept as a repo-owned overlay so the submodule can stay aligned with upstream MinerU releases.
