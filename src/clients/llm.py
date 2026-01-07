import json
from typing import Any, TypeVar

from litellm import completion
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

# TODO: pass key and url

#
class LLMClient:
    def __init__(self, model_name: str, temperature: float, system_prompt: str):
        self.model_name = model_name
        self.temperature = temperature
        self.system_prompt = system_prompt.strip()

    def _build_message(
        self, payload: Any, user_prefix: str | None
    ) -> list[dict[str, str]]:
        user_prefix = user_prefix or ""
        messages = [
            {"role": "system", "content": self.system_prompt},
            {
                "role": "user",
                "content": user_prefix + json.dumps(payload, ensure_ascii=False),
            },
        ]
        return messages

    def structured_call(
        self,
        response_format: type[T],
        payload: Any,
        user_prefix: str | None = None,
    ):
        messages = self._build_message(payload=payload, user_prefix=user_prefix)

        # https://docs.litellm.ai/docs/completion/json_mode#pass-in-json_schema
        resp = completion(
            model=self.model_name,
            messages=messages,
            response_format=response_format,
            temperature=self.temperature,
        )
        content = (resp.choices[0].message.content or "").strip()  # type: ignore
        json_data = json.loads(content)
        return response_format.model_validate(json_data)
