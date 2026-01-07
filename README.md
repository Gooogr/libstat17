# libstat17

## Setup

### Install dependencies
```bash
poetry install
```
### Configure environment

Run:

`python scripts/create_vk_token.py`

Save the service token to `.env`.

### Configure LLM provider

Set API keys for your LLM provider in `.env`. 
More details: https://docs.litellm.ai/docs/set_keys#environment-variables

## Run pipeline
`snakemake --cores 1`

### LLM-based steps
Also part of snakemake pipeleine, but could be run separately

#### Topics labeling<br> 
```bash
python ./scripts/label_topics.py --out-csv ./data/processed/topic_labels.csv
```
