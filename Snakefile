import pandas as pd
from functools import lru_cache

PLACES_CSV = "data/external/map_points.csv"
OUTDIR = "data/external/groups"

rule all:
    input:
        lambda wc: expand(
            f"{OUTDIR}/place_{{place_id}}.json",
            place_id=table()["place_id"].tolist(),
        )

checkpoint maps_points:
    output:
        PLACES_CSV
    shell:
        "python scripts/get_map_points.py --out {output}"

@lru_cache
def table():
    path = checkpoints.maps_points.get().output[0]
    df = pd.read_csv(path, dtype=str)
    df["place_id"] = df["number"].astype(str)
    df["url"] = df["link"].astype(str)
    # index is what we use for lookup; still keep place_id column for listing
    return df[["place_id", "url"]].set_index("place_id", drop=False)

rule vk_data:
    input:
        PLACES_CSV
    output:
        f"{OUTDIR}/place_{{place_id}}.json"
    params:
        group=lambda wc: table().at[wc.place_id, "url"]
    resources:
        vk_api=1
    shell:
        "python scripts/get_group_data.py --group '{params.group}' --out {output}"
