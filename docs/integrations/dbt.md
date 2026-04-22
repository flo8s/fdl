# dbt

fdl provides first-class integration with [dbt](https://www.getdbt.com/).
fdl manages the DuckLake catalog and environment, while dbt handles data transformation.

## Project Structure

A typical fdl + dbt project looks like this:

```
├── fdl.toml            # fdl config (name, targets)
├── dbt_project.yml     # dbt config (models, materialization)
├── profiles.yml        # DuckDB + DuckLake connection
├── pipeline.py         # pipeline entry point
├── models/             # dbt models
└── .fdl/               # catalog (gitignored)
```

## DuckLake Connection

### profiles.yml

dbt connects to a DuckLake catalog via dbt-duckdb's `attach` feature.
`fdl run` injects the environment variables that profiles.yml references:

```yaml
my_dataset:
  target: default
  outputs:
    default:
      type: duckdb
      path: ":memory:"
      database: my_dataset
      schema: main
      threads: 1
      extensions:
        - httpfs
        - ducklake
      settings:
        s3_url_style: path
        s3_access_key_id: "{{ env_var('FDL_S3_ACCESS_KEY_ID', '') }}"
        s3_secret_access_key: "{{ env_var('FDL_S3_SECRET_ACCESS_KEY', '') }}"
        s3_endpoint: "{{ env_var('FDL_S3_ENDPOINT_HOST', '') }}"
        s3_region: auto
      attach:
        - path: "ducklake:{{ env_var('FDL_CATALOG_URL') }}"
          alias: my_dataset
          is_ducklake: true
          options:
            DATA_PATH: "{{ env_var('FDL_DATA_URL') }}"
            OVERRIDE_DATA_PATH: true
```

### Environment Variables

`fdl run` automatically injects these variables:

| Variable | Description |
|----------|-------------|
| `FDL_CATALOG_URL` | DuckLake catalog connection URL (`sqlite:///<abs>`) |
| `FDL_DATA_URL` | Parquet data files directory (URL or local path) |
| `FDL_S3_ACCESS_KEY_ID` | S3 access key (S3 targets only) |
| `FDL_S3_SECRET_ACCESS_KEY` | S3 secret key (S3 targets only) |
| `FDL_S3_ENDPOINT_HOST` | S3 endpoint hostname (S3 targets only) |

## Workflow

### Local development

```bash
fdl run default -- dbt run
```

### CI/CD (GitHub Actions)

```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      FDL_S3_ENDPOINT: https://${{ secrets.CF_ACCOUNT_ID }}.r2.cloudflarestorage.com
      FDL_S3_ACCESS_KEY_ID: ${{ secrets.S3_ACCESS_KEY_ID }}
      FDL_S3_SECRET_ACCESS_KEY: ${{ secrets.S3_SECRET_ACCESS_KEY }}
      FDL_S3_BUCKET: ${{ vars.S3_BUCKET }}
    steps:
      - uses: actions/checkout@v5
      - uses: astral-sh/setup-uv@v7
      - run: uv sync

      - run: uv run fdl pull default
      - run: uv run fdl run default -- dbt run
      - run: uv run fdl push default
```
