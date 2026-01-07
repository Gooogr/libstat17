# libstat17

## Set up
`poetry install`

Run `python scripts/create_vk_token.py` and save service token in `.env` file

Set API keys for your LLM provider in `.env` file. More details [here](https://docs.litellm.ai/docs/set_keys#environment-variables)

## Run pipeline
`snakemake --cores 1`
