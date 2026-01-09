import asyncio
import json
from typing import Any, TypeVar

from litellm import acompletion
from loguru import logger
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class LLMClient:
    def __init__(
        self,
        model_name: str,
        temperature: float,
        system_prompt: str,
        max_concurrency: int = 5,
        n_retries: int = 2,
        retry_delay_s: float = 1.0,
        **completion_kwargs: Any,
    ):
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if n_retries < 0:
            raise ValueError("n_retries must be >= 0")
        if retry_delay_s < 0:
            raise ValueError("retry_delay_s must be >= 0")

        self.model_name = model_name
        self.temperature = temperature
        self.system_prompt = system_prompt.strip()

        self.n_retries = n_retries
        self.retry_delay_s = retry_delay_s

        self._sem = asyncio.Semaphore(max_concurrency)
        self._completion_kwargs = completion_kwargs  # forwarded to LiteLLM

    def _build_messages(
        self, payload: Any, user_prefix: str | None
    ) -> list[dict[str, str]]:
        prefix = user_prefix or ""
        return [
            {"role": "system", "content": self.system_prompt},
            {
                "role": "user",
                "content": prefix + json.dumps(payload, ensure_ascii=False),
            },
        ]

    async def structured_call_async(
        self,
        response_format: type[T],
        payload: Any,
        user_prefix: str | None = None,
    ) -> T:
        messages = self._build_messages(payload=payload, user_prefix=user_prefix)
        total_attempts = 1 + self.n_retries

        last_content: str | None = None
        last_err: Exception | None = None

        for attempt in range(1, total_attempts + 1):
            try:
                async with self._sem:
                    resp = await acompletion(
                        model=self.model_name,
                        messages=messages,
                        response_format=response_format,
                        temperature=self.temperature,
                        **self._completion_kwargs,
                    )

                last_content = (resp.choices[0].message.content or "").strip()  # type: ignore[attr-defined]
                if not last_content:
                    raise ValueError("Empty response content from provider")

                return response_format.model_validate_json(last_content)

            except Exception as e:
                last_err = e

                if attempt < total_attempts:
                    snippet = (last_content or "")[:800].replace("\n", "\\n")
                    logger.warning(
                        "LLM JSON parse/validation failed (attempt {}/{}). Retrying in {}s. "
                        "model={} err={} content_snippet={}",
                        attempt,
                        total_attempts,
                        self.retry_delay_s,
                        self.model_name,
                        repr(e),
                        snippet,
                    )
                    if self.retry_delay_s:
                        await asyncio.sleep(self.retry_delay_s)
                    continue

                snippet = (last_content or "")[:800].replace("\n", "\\n")
                logger.error(
                    "LLM JSON parse/validation failed after {} attempts. Returning empty output. "
                    "model={} err={} content_snippet={}",
                    total_attempts,
                    self.model_name,
                    repr(e),
                    snippet,
                )

                # "Empty output" fallback (works for your {"rows": [...]} response models)
                try:
                    return response_format.model_validate({"rows": []})
                except Exception:
                    return response_format.model_construct()  # type: ignore[return-value]

        # Should never happen, but keep a safe fallback.
        logger.error(
            "Unexpected control flow in structured_call_async: {}", repr(last_err)
        )
        try:
            return response_format.model_validate({"rows": []})
        except Exception:
            return response_format.model_construct()  # type: ignore[return-value]

    def structured_call(
        self,
        response_format: type[T],
        payload: Any,
        user_prefix: str | None = None,
    ) -> T:
        """
        Sync convenience wrapper for backward compatibility
        - In sync code: runs the async method via asyncio.run()
        - In async code: raises (use `await structured_call_async(...)`)
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.structured_call_async(
                    response_format=response_format,
                    payload=payload,
                    user_prefix=user_prefix,
                )
            )

        raise RuntimeError(
            "structured_call() cannot be used from an async context. "
            "Use: await structured_call_async(...)"
        )
