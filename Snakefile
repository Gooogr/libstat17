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
BOOK_WISHES_CSV = f"{PROCESSED_DIR}/book_wishes.csv"
NONBOOK_WISHES_CSV = f"{PROCESSED_DIR}/nonbook_wishes.csv"
PLACE_METRICS_CSV = f"{PROCESSED_DIR}/place_metrics.csv"


rule all:
    input:
        TOPIC_LABELS_CSV,
        BOOK_WISHES_CSV,
        NONBOOK_WISHES_CSV,
        PLACE_METRICS_CSV,


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
        "mkdir -p {PROCESSED_DIR} && "
        "python scripts/topic_labeling.py "
        "--topics-csv {input.topics} "
        "--messages-csv {input.messages} "
        "--out-csv {output} "
        "--max-places 25"


rule book_wishes:
    input:
        topic_labels=TOPIC_LABELS_CSV,
        topics=TOPICS_CSV,
        messages=MESSAGES_CSV
    output:
        BOOK_WISHES_CSV
    shell:
        "python scripts/extract_wishes_books.py "
        "--topic-labels-csv {input.topic_labels} "
        "--topics-csv {input.topics} "
        "--messages-csv {input.messages} "
        "--out-csv {output}"


rule nonbook_wishes:
    input:
        topic_labels=TOPIC_LABELS_CSV,
        topics=TOPICS_CSV,
        messages=MESSAGES_CSV
    output:
        NONBOOK_WISHES_CSV
    shell:
        "python scripts/extract_wishes_nonbooks.py "
        "--topic-labels-csv {input.topic_labels} "
        "--topics-csv {input.topics} "
        "--messages-csv {input.messages} "
        "--out-csv {output}"


rule place_metrics:
    input:
        places=PLACES_CSV
    output:
        PLACE_METRICS_CSV
    shell:
        "mkdir -p {PROCESSED_DIR} && "
        "python scripts/extract_place_metrics.py "
        "--places-csv {input.places} "
        "--out-csv {output} "
        "--max-places 25"
