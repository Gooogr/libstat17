MAP_POINTS_CSV_PATH = "data/external/map_points.csv"
VK_DATA_OUTDIR = "data/external/groups"
VK_DATA_DONE = f"{VK_DATA_OUTDIR}/.done" # marker for ended process

rule all:
    input:
        VK_DATA_DONE

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
    resources:
        vk_api=1
    shell:
        "mkdir -p {VK_DATA_OUTDIR} && "
        "python scripts/get_group_data.py --csv {input} --outdir {VK_DATA_OUTDIR} && "
        "touch {output}"
