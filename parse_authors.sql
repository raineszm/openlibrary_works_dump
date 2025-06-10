-- Load the CSV file and extract JSON data from column4
COPY (
    SELECT json_data->>'key' as key,
        json_data->>'name' as name,
        -- Handle bio which might be a text block
        CASE
            WHEN json_data->>'bio' IS NULL THEN NULL
            WHEN json_data->>'bio' LIKE '{"type":%' THEN TRY_CAST(json_data->>'bio' AS JSON)->>'value'
            ELSE json_data->>'bio'
        END as bio,
        json_data->>'birth_date' as birth_date,
        json_data->>'death_date' as death_date,
        json_data->>'location' as location,
        json_data->>'date' as date,
        json_data->>'entity_type' as entity_type,
        json_data->>'fuller_name' as fuller_name,
        json_data->>'personal_name' as personal_name,
        json_data->>'title' as title,
        json_data->'alternate_names'->>'$[*]' as alternate_names,
        json_data->'links'->>'$[*]' as links,
        json_data->'remote_ids'->>'wikidata' as wikidata_id,
        json_data->'remote_ids'->>'viaf' as viaf_id,
        FROM (
            SELECT column4::JSON as json_data
            FROM read_csv_auto(
                    'data/ol_dump_2025-01-08/ol_dump_authors_2025-01-08.txt.gz'
                )
        )
    WHERE (json_data->'entity_type') is NULL
        OR json_extract_string(json_data, '$.entity_type') = 'person'
) TO 'data/upload/ol_authors.parquet' (
    FORMAT parquet,
    COMPRESSION ZSTD,
    COMPRESSION_LEVEL 15
);