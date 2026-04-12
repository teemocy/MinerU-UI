# API Overlay

This directory contains the repo-owned MinerU API/runtime overrides.

Current overrides:

- `mineru/utils/config_reader.py`
- `mineru/utils/model_utils.py`
- `mineru/backend/vlm/vlm_analyze.py`

These files are copied over the upstream submodule during image builds or when running:

```bash
./scripts/apply_customizations.sh third_party/MinerU api
```

The intent is to keep the upstream repo clean while still making the API delta explicit and reviewable.
