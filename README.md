# Open Library Data Processing

This project processes Open Library data dumps to create optimized Parquet files
for analysis. It uses DuckDB for efficient JSON processing and Snakemake for
workflow management.

## Prerequisites

- [pixi](https://pixi.sh/) for environment management

## Setup

1. Install pixi if you haven't already:
   ```bash
   curl -fsSL https://pixi.sh/install.sh | bash
   ```

2. Set up the pixi environment
   ```bash
   pixi install
   ```

## Getting the Data

1. Download the Open Library works dump from
   [Open Library Data Dumps](https://openlibrary.org/developers/dumps)
   - Look for the "works dump" (~2.9GB)
   - The file will be named something like `ol_dump_works_YYYY-MM-DD.txt.gz`
   - You may also consider using the torrents provided by the Internet Archive.

2. Make sure the data is in the correct location, e.g.,
   ```bash
   mkdir -p data/ol_dump_2025-01-08
   mv ol_dump_works_2025-01-08.txt.gz data/ol_dump_2025-01-08/
   ```

## Running the Pipeline

```bash
pixi run snakemake -c CORES
```

where `CORES` is the number of cores you want to use to run the process.

This will:

- Process the works dump using DuckDB
- Extract relevant fields from the JSON data
- Create an optimized Parquet file at `data/ol_works.parquet`

## Output

The pipeline generates a Parquet file (`data/ol_works.parquet`) containing the
following selection of fields:

- key
- description
- title
- subtitle
- authors
- location
- first_publish_date
- first_sentence
- subjects
- subject_places
- subject_people
- subject_times
- lc_classifications
- dewey_number

**NB**: The format of the fields may not be uniform, despite what the Open
Library schema claims.
