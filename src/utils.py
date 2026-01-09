import re

from src.clients.llm import LLMClient


# https://stackoverflow.com/a/58356570
def remove_emojis(text: str) -> str:
    emoj = re.compile(
        "["
        "\U0001f600-\U0001f64f"  # emoticons
        "\U0001f300-\U0001f5ff"  # symbols & pictographs
        "\U0001f680-\U0001f6ff"  # transport & map symbols
        "\U0001f1e0-\U0001f1ff"  # flags (iOS)
        "\U00002500-\U00002bef"  # chinese char
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2b55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"  # dingbats
        "\u3030"
        "]+",
        re.UNICODE,
    )
    return re.sub(emoj, "", text)


def create_llm_client(
    model_name: str,
    temperature: float,
    system_prompt: str,
    max_concurrency: int = 20,
    n_retries: int = 2,
    retry_delay_s: float = 10.0,
) -> LLMClient:
    """Create configured LLM client."""
    import litellm

    litellm.drop_params = True  # OpenAI APIs could skip temperature

    return LLMClient(
        model_name=model_name,
        temperature=temperature,
        system_prompt=system_prompt,
        max_concurrency=max_concurrency,
        n_retries=n_retries,
        retry_delay_s=retry_delay_s,
    )
