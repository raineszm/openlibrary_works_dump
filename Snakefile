rule all:
    input:
        "data/ol_works.parquet",
        "data/ol_authors.parquet"

rule convert_works:
    input:
        "data/ol_dump_2025-01-08/ol_dump_works_2025-01-08.txt.gz"
    output:
        "data/ol_works.parquet"
    shell:
        "duckdb -f parse_works.sql"

rule convert_authors:
    input:
        "data/ol_dump_2025-01-08/ol_dump_authors_2025-01-08.txt.gz"
    output:
        "data/ol_authors.parquet"
    shell:
        "duckdb -f parse_authors.sql"