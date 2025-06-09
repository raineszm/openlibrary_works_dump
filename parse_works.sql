-- Load the CSV file and extract JSON data from column4
COPY (
    SELECT json_data->>'key' as key,
        -- Handle both plain text and JSON-wrapped descriptions safely
        CASE
            WHEN json_data->>'description' IS NULL THEN NULL
            WHEN json_data->>'description' LIKE '{"type":%' THEN TRY_CAST(json_data->>'description' AS JSON)->>'value'
            ELSE json_data->>'description'
        END as description,
        json_data->>'title' as title,
        json_data->>'subtitle' as subtitle,
        json_data->'authors'->>'$[*].author.key' as authors,
        json_data->>'location' as location,
        json_data->>'first_publish_date' as first_publish_date,
        json_data->'first_sentence'->>'value' as first_sentence,
        -- Classification arrays
        json_data->'subjects'->>'$[*]' as subjects,
        json_data->'subject_places'->>'$[*]' as subject_places,
        json_data->'subject_people'->>'$[*]' as subject_people,
        json_data->'subject_times'->>'$[*]' as subject_times,
        json_data->'lc_classifications'->>'$[*]' as lc_classifications,
        json_data->'dewey_number'->>'$[*]' as dewey_number
    FROM (
            SELECT column4::JSON as json_data
            FROM read_csv_auto(
                    'data/ol_dump_2025-01-08/ol_dump_works_2025-01-08.txt.gz'
                )
        )
) TO 'data/ol_works.parquet' (
    FORMAT parquet,
    COMPRESSION ZSTD,
    COMPRESSION_LEVEL 15
);