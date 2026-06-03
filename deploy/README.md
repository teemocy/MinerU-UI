# Deploy Assets

This directory holds the repo-owned deployment assets.

- `docker/`: image definitions built from `this repo + third_party/MinerU`
- `scripts/`: helper scripts for single-service deployment
- `compose.ocr_vlm.yaml`: full stack for `mineru-openai-server`, `mineru-api`, and the custom OCR WebUI
- `compose.ocr_single_gpu.yaml`: single-GPU stack where CUDA `mineru-api`
  owns both VLM and pipeline models and the WebUI defaults to
  `hybrid-auto-engine`

In the compose stack, choose `vlm-http-client` or `hybrid-http-client` in the
WebUI so OCR requests use the `mineru-openai-server` container. The `*-auto-engine`
backends run VLM locally inside `mineru-api`.

For one GPU, prefer the single-GPU stack if you want `hybrid-auto-engine` to use
GPU acceleration without a separate OpenAI-compatible VLM server:

```bash
docker compose -f deploy/compose.ocr_single_gpu.yaml up -d --build
```

This deployment removes `mineru-openai-server`; `mineru-api` is built from a
vLLM CUDA image and receives the GPU reservation directly. Defaults are
conservative for 16 GB cards: one API request at a time, hybrid batch ratio 1,
processing window 16, and vLLM GPU memory utilization 0.5. Tune with
`MINERU_VLLM_GPU_MEMORY_UTILIZATION`, `MINERU_HYBRID_BATCH_RATIO`, and
`MINERU_PROCESSING_WINDOW_SIZE` after confirming VRAM headroom.

The WebUI's default chunk size is controlled by
`MINERU_WEBUI_MAX_PAGES_PER_CHUNK` and can also be changed per job in the
Execution Settings panel. Larger chunks reduce the number of API tasks but
increase per-task memory use and retry cost.

The WebUI's default VLM/hybrid processing window is controlled by
`MINERU_WEBUI_PROCESSING_WINDOW_SIZE` and can also be changed per job. Larger
windows reduce intra-chunk VLM passes but increase peak memory use.

The WebUI's default per-chunk timeout is controlled by
`MINERU_WEBUI_CHUNK_TIMEOUT_SECONDS` and can also be changed per job. Large
chunks, such as 899-page chunks, need a larger timeout than the old 7200-second
default.
