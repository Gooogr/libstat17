# scripts/extract_place_metrics.py

import argparse
import asyncio
from itertools import islice
from pathlib import Path

import litellm
import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict
from tqdm import tqdm

from src.clients.llm import LLMClient
from src.prompts.lib_stats import LIBS_METRICS_EXTRACTION_PROMPT
from src.utils import remove_emojis

load_dotenv()

MODEL_NAME = "openai/gpt-5-mini"
TEMPERATURE = 0.0


# --- Input payload model --- #
class PlacePayload(BaseModel):
    place_id: int
    place_name: str
    description: str


# --- Output response model --- #
class PlaceMetricsRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    place_id: int
    readers_count: int | None
    book_collection_size: int | None
    is_school_library: bool


class PlaceMetricsResponse(BaseModel):
    rows: list[PlaceMetricsRow]


def batch_payload(items: list[PlacePayload], max_places: int = 25):
    it = iter(items)
    chunk = list(islice(it, max_places))
    while chunk:
        yield chunk
        chunk = list(islice(it, max_places))


def _clean_text(s: str) -> str:
    s = (s or "").strip()
    s = remove_emojis(s)
    s = " ".join(s.split())
    return s


def load_places(places_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(places_csv)

    required = {"place_id", "place_name", "description"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"places.csv missing required columns: {sorted(missing)}")

    df["place_id"] = df["place_id"].astype(int)
    df["place_name"] = df["place_name"].fillna("").astype(str).map(_clean_text)
    df["description"] = df["description"].fillna("").astype(str).map(_clean_text)

    df = df.sort_values("place_id").reset_index(drop=True)
    return df


def build_place_payloads(df: pd.DataFrame) -> list[PlacePayload]:
    places: list[PlacePayload] = []
    for r in df[["place_id", "place_name", "description"]].itertuples(index=False):
        places.append(
            PlacePayload(
                place_id=int(r.place_id),
                place_name=str(r.place_name),
                description=str(r.description),
            )
        )
    places.sort(key=lambda p: p.place_id)
    return places


async def main_async() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--places-csv", default="data/interim/places.csv")
    ap.add_argument("--out-csv", default="data/processed/place_metrics.csv")
    ap.add_argument("--max-places", type=int, default=25)
    ap.add_argument("--max-concurrency", type=int, default=20)
    ap.add_argument("--n-retries", type=int, default=2)
    ap.add_argument("--retry-delay-s", type=float, default=1.0)
    args = ap.parse_args()

    places_csv = Path(args.places_csv)
    if not places_csv.exists():
        raise FileNotFoundError(f"places.csv not found: {places_csv}")

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    litellm.drop_params = True  # some providers ignore temperature, etc.

    client = LLMClient(
        model_name=MODEL_NAME,
        temperature=TEMPERATURE,
        system_prompt=LIBS_METRICS_EXTRACTION_PROMPT,
        max_concurrency=args.max_concurrency,
        n_retries=args.n_retries,
        retry_delay_s=args.retry_delay_s,
    )

    places_df = load_places(places_csv)
    if places_df.empty:
        raise SystemExit("No places to process (places.csv is empty after loading).")

    places = build_place_payloads(places_df)
    if not places:
        raise SystemExit("No valid places to process (no place_id rows).")

    batches = list(batch_payload(places, max_places=args.max_places))
    tasks: list[asyncio.Task[PlaceMetricsResponse]] = []

    for i, batch in enumerate(batches, start=1):
        print(f"[batch {i}/{len(batches)}] places={len(batch)}")

        tasks.append(
            asyncio.create_task(
                client.structured_call_async(
                    response_format=PlaceMetricsResponse,
                    payload=[b.model_dump() for b in batch],
                    user_prefix="ВХОД:\n",
                )
            )
        )

    all_rows: list[PlaceMetricsRow] = []
    for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        resp = await fut
        all_rows.extend(resp.rows)

    metrics_df = pd.DataFrame([r.model_dump() for r in all_rows])
    if metrics_df.empty:
        raise SystemExit("LLM returned no rows.")

    # Keep exactly one row per place_id (prefer the first occurrence)
    metrics_df = (
        metrics_df.sort_values("place_id")
        .drop_duplicates(subset=["place_id"], keep="first")
        .reset_index(drop=True)
    )

    # Ensure column order
    metrics_df = metrics_df[
        ["place_id", "readers_count", "book_collection_size", "is_school_library"]
    ]
    
    df_out = places_df.merge(metrics_df, on="place_id", how="left")
    df_out = df_out.drop(columns="description")

    df_out.to_csv(out_csv, index=False)
    print(f"Wrote {len(metrics_df)} rows -> {out_csv}")

    n = len(metrics_df)
    p_null_readers = float(metrics_df["readers_count"].isna().mean()) if n else 0.0
    p_null_fund = float(metrics_df["book_collection_size"].isna().mean()) if n else 0.0
    p_null_school = float(metrics_df["is_school_library"].isna().mean()) if n else 0.0
    print(
        "Stats:",
        f"places={n}",
        f"readers_count_null={p_null_readers:.2%}",
        f"book_collection_size_null={p_null_fund:.2%}",
        f"is_school_library_null={p_null_school:.2%}",
    )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

