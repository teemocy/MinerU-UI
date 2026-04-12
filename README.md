# MinerU-UI

This repository keeps the custom MinerU OCR layers separate from upstream MinerU.

## Layout

- `third_party/MinerU/`: upstream MinerU git submodule, sourced from `https://github.com/opendatalab/MinerU.git`
- `api/`: repo-owned overrides for the API/runtime files that differ from upstream
- `webui/`: repo-owned leak-safe OCR WebUI sources
- `deploy/`: Dockerfiles, compose stack, and deploy scripts
- `scripts/apply_customizations.sh`: overlays `api/` and `webui/` onto a MinerU source tree
- `scripts/sync_mineru.sh`: syncs the submodule to the newest upstream `mineru-*` release tag

## Why This Layout

The goal is to avoid carrying a full MinerU fork in this repository.

- Upstream MinerU stays in a clean submodule.
- Your API and WebUI code live in this repo.
- Docker builds apply the overlays at build time, so updating MinerU is mostly a submodule sync plus regression testing.

## Sync Upstream MinerU

```bash
./scripts/sync_mineru.sh
```

That updates `third_party/MinerU` to the newest tag matching `mineru-*`.

You can override the tag pattern if needed:

```bash
MINERU_TAG_PATTERN='mineru-*' ./scripts/sync_mineru.sh
```

## Apply Local Customizations Manually

If you want a locally customized MinerU tree in-place:

```bash
./scripts/apply_customizations.sh third_party/MinerU all
```

Supported modes are:

- `api`
- `webui`
- `all`

## Docker Workflows

Start the full OCR VLM stack:

```bash
docker compose -f deploy/compose.ocr_vlm.yaml up -d --build
```

Start only the WebUI against an already-running MinerU API:

```bash
API_URL=http://host.docker.internal:8000 \
WEBUI_PORT=7861 \
./deploy/scripts/deploy_ocr_webui.sh
```

## Notes

- The API overrides currently live under `api/mineru/...`.
- The custom WebUI sources currently live under `webui/mineru/...`.
- The build path intentionally applies overlays onto a staged MinerU tree instead of editing the submodule by default.
