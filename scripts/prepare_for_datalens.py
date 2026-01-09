import argparse
import shutil
from pathlib import Path

import pandas as pd


LAT_COL = "lat"
LON_COL = "lon"
GEOPOINT_COL = "geopoint"


def _ensure_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")


def _build_geopoint(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    # DataLens-friendly geopoint as JSON list string: "[lat,lon]" (lat first)
    lat = (
        lat_series.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(",", ".", regex=False)
    )
    lon = (
        lon_series.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(",", ".", regex=False)
        .replace({"nan": "", "None": "", "": ""})
    )

    lat_num = pd.to_numeric(lat, errors="coerce")
    lon_num = pd.to_numeric(lon, errors="coerce")

    def fmt(a, b) -> str:
        if pd.isna(a) or pd.isna(b):
            return ""
        return f"[{a:.8f},{b:.8f}]"

    return pd.Series([fmt(a, b) for a, b in zip(lat_num, lon_num)])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", default="data/processed")
    ap.add_argument("--out-dir", default="data/datalens")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    place_metrics_in = input_dir / "place_metrics.csv"
    wishes_books_in = input_dir / "wishes_books.csv"
    wishes_nonbooks_in = input_dir / "wishes_nonbook.csv"

    _ensure_exists(place_metrics_in)
    _ensure_exists(wishes_books_in)
    _ensure_exists(wishes_nonbooks_in)

    # --- place_dl.csv: add geopoint column ---
    df = pd.read_csv(place_metrics_in)

    if LAT_COL not in df.columns or LON_COL not in df.columns:
        raise ValueError(
            f"Places data must contain '{LAT_COL}' and '{LON_COL}'. "
            f"Found: {list(df.columns)}"
        )

    df[GEOPOINT_COL] = _build_geopoint(df[LAT_COL], df[LON_COL])

    place_out = out_dir / "places.csv"
    df.to_csv(place_out, index=False)

    # --- copy wishes as-is ---
    books_out = out_dir / "wishes_book.csv"
    nonbooks_out = out_dir / "wishes_nonbook.csv"
    shutil.copy2(wishes_books_in, books_out)
    shutil.copy2(wishes_nonbooks_in, nonbooks_out)

if __name__ == "__main__":
    main()
