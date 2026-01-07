
EXTERNAL_DIR = "data/external"
VK_DATA_OUTDIR = f"{EXTERNAL_DIR}/groups"
MAP_POINTS_CSV_PATH = f"{EXTERNAL_DIR}/map_points.csv"
VK_DATA_DONE = f"{VK_DATA_OUTDIR}/.done"  # marker for ended process

INTERIM_DIR = "data/interim"
PLACES_CSV = f"{INTERIM_DIR}/places.csv"
TOPICS_CSV = f"{INTERIM_DIR}/topics.csv"
MESSAGES_CSV = f"{INTERIM_DIR}/messages.csv"

PROCESSED_DIR = "data/processed"
TOPIC_LABELS_CSV = f"{PROCESSED_DIR}/topic_labels.csv"


rule all:
    input:
        TOPIC_LABELS_CSV


rule map_points:
    output:
        MAP_POINTS_CSV_PATH
    shell:
        "python scripts/get_map_points.py --out {output}"


rule vk_data_all:
    input:
        MAP_POINTS_CSV_PATH
    output:
        VK_DATA_DONE
    shell:
        "mkdir -p {VK_DATA_OUTDIR} && "
        "python scripts/get_group_data.py --csv {input} --outdir {VK_DATA_OUTDIR} && "
        "touch {output}"


rule flatten_groups:
    input:
        geo=MAP_POINTS_CSV_PATH,
        done=VK_DATA_DONE
    output:
        places=PLACES_CSV,
        topics=TOPICS_CSV,
        messages=MESSAGES_CSV
    shell:
        "python scripts/flatten_groups.py "
        "--geo-csv {input.geo} "
        "--groups-dir {VK_DATA_OUTDIR} "
        "--output-dir {INTERIM_DIR}"


rule topic_labels:
    input:
        topics=TOPICS_CSV,
        messages=MESSAGES_CSV
    output:
        TOPIC_LABELS_CSV
    shell:
        "mkdir -p data/processed && "
        "python scripts/topic_labeling.py "
        "--topics-csv {input.topics} "
        "--messages-csv {input.messages} "
        "--out-csv {output} "
        "--max-places 25"
