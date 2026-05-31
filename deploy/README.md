# Deploy Assets

This directory holds the repo-owned deployment assets.

- `docker/`: image definitions built from `this repo + third_party/MinerU`
- `scripts/`: helper scripts for single-service deployment
- `compose.ocr_vlm.yaml`: full stack for `mineru-openai-server`, `mineru-api`, and the custom OCR WebUI

In the compose stack, choose `vlm-http-client` or `hybrid-http-client` in the
WebUI so OCR requests use the `mineru-openai-server` container. The `*-auto-engine`
backends run VLM locally inside `mineru-api`.
