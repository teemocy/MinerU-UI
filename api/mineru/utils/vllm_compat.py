from __future__ import annotations

import inspect
import uuid
from typing import Any

from loguru import logger

_PATCHED = False


def _renderer_needs_tokenize_params(render_method: Any) -> bool:
    try:
        return "tok_params" in inspect.signature(render_method).parameters
    except (TypeError, ValueError):
        return False


def _build_tokenize_params(client: Any, sampling_params: Any = None) -> Any:
    from vllm.renderers import TokenizeParams

    model_max_length = getattr(client, "model_max_length", None)
    requested_output_tokens = getattr(sampling_params, "max_tokens", None)
    if (
        requested_output_tokens is not None
        and model_max_length is not None
        and requested_output_tokens < model_max_length
    ):
        max_output_tokens = requested_output_tokens
    else:
        max_output_tokens = 0
    return TokenizeParams(
        max_total_tokens=model_max_length,
        max_output_tokens=max_output_tokens,
        max_total_tokens_param="max_model_len",
        max_output_tokens_param="max_tokens",
    )


async def _render_vllm_cmpl_input(client: Any, raw_prompt: dict[str, Any], sampling_params: Any = None) -> dict[str, Any]:
    renderer = getattr(client.vllm_async_llm, "renderer", None)
    if renderer is None:
        return raw_prompt

    render_cmpl_async = getattr(renderer, "render_cmpl_async", None)
    if callable(render_cmpl_async):
        if _renderer_needs_tokenize_params(render_cmpl_async):
            rendered_inputs = await render_cmpl_async(
                [raw_prompt],
                _build_tokenize_params(client, sampling_params),
            )
        else:
            rendered_inputs = await render_cmpl_async([raw_prompt])
        return rendered_inputs[0]

    render_cmpl = getattr(renderer, "render_cmpl", None)
    if callable(render_cmpl):
        if _renderer_needs_tokenize_params(render_cmpl):
            rendered_inputs = render_cmpl(
                [raw_prompt],
                _build_tokenize_params(client, sampling_params),
            )
        else:
            rendered_inputs = render_cmpl([raw_prompt])
        return rendered_inputs[0]

    return raw_prompt


async def _aio_predict(
    self: Any,
    image: Any,
    prompt: str = "",
    sampling_params: Any = None,
    priority: int | None = None,
) -> str:
    from mineru_vl_utils.vlm_client.base_client import ServerError
    from mineru_vl_utils.vlm_client.utils import aio_image_to_obj_list
    from mineru_vl_utils.vlm_client.vllm_engine_client import _build_raw_vllm_prompt

    image = await aio_image_to_obj_list(image)

    chat_prompt: str = self.tokenizer.apply_chat_template(
        self.build_messages(prompt, len(image)),
        tokenize=False,
        add_generation_prompt=True,
    )

    vllm_sp = self.build_vllm_sampling_params(sampling_params)

    generate_kwargs = {}
    if priority is not None:
        generate_kwargs["priority"] = priority

    vllm_prompt = await self._render_vllm_cmpl_input(
        _build_raw_vllm_prompt(chat_prompt, image),
        vllm_sp,
    )

    last_output = None
    async for output in self.vllm_async_llm.generate(
        prompt=vllm_prompt,
        sampling_params=vllm_sp,
        request_id=str(uuid.uuid4()),
        **generate_kwargs,
    ):
        last_output = output

    if last_output is None:
        raise ServerError("No output from the server.")

    return self.get_output_content(last_output)


def patch_mineru_vl_utils_vllm_async_renderer() -> None:
    global _PATCHED
    if _PATCHED:
        return

    try:
        from mineru_vl_utils.vlm_client.vllm_async_engine_client import VllmAsyncEngineVlmClient
    except Exception as exc:
        logger.debug(f"Skipped vLLM async renderer compatibility patch: {exc}")
        return

    if getattr(VllmAsyncEngineVlmClient, "_mineru_ui_renderer_compat", False):
        _PATCHED = True
        return

    VllmAsyncEngineVlmClient._render_vllm_cmpl_input = _render_vllm_cmpl_input
    VllmAsyncEngineVlmClient.aio_predict = _aio_predict
    VllmAsyncEngineVlmClient._mineru_ui_renderer_compat = True
    _PATCHED = True
    logger.info("Installed vLLM async renderer compatibility patch.")
