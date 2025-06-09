COPY (
    WITH english_editions AS (
        SELECT *
        FROM (
                SELECT column4::JSON as json_data
                FROM read_csv_auto(
                        'data/ol_dump_2025-01-08/ol_dump_editions_2025-01-08.txt.gz',
                        max_line_size = 10000000
                    )
            )
        WHERE (json_data->'languages'->>'$[*].key').contains('/languages/eng')
    ),
    first_english_edition AS (
        SELECT FIRST(
                json_data
                ORDER BY json_data->'publish_date' DESC
            ) json_data
        FROM english_editions
        GROUP BY json_data->'works'->>'$[0].key'
    )
    SELECT json_data->>'key' as key,
        json_data->>'title' as title,
        json_data->>'subtitle' as subtitle,
        -- Extract work key from the works array
        json_data->'works'->>'$[0].key' as work_key,
        -- Handle text blocks (description, first_sentence, notes)
        CASE
            WHEN json_data->>'description' IS NULL THEN NULL
            WHEN json_data->>'description' LIKE '{"type":%' THEN TRY_CAST(json_data->>'description' AS JSON)->>'value'
            ELSE json_data->>'description'
        END as description,
        -- CASE
        --     WHEN json_data->>'first_sentence' IS NULL THEN NULL
        --     WHEN json_data->>'first_sentence' LIKE '{"type":%' THEN TRY_CAST(json_data->>'first_sentence' AS JSON)->>'value'
        --     ELSE json_data->>'first_sentence'
        -- END as first_sentence,
        -- CASE
        --     WHEN json_data->>'notes' IS NULL THEN NULL
        --     WHEN json_data->>'notes' LIKE '{"type":%' THEN TRY_CAST(json_data->>'notes' AS JSON)->>'value'
        --     ELSE json_data->>'notes'
        -- END as notes,
        -- Extract author keys from the authors array
        json_data->'authors'->>'$[*].key' as authors,
        -- ISBNs and other identifiers
        json_data->'isbn_10'->>'$[*]' as isbn_10,
        json_data->'isbn_13'->>'$[*]' as isbn_13,
        json_data->'lccn'->>'$[*]' as lccn,
        json_data->>'ocaid' as ocaid,
        json_data->'oclc_numbers'->>'$[*]' as oclc_numbers,
        -- json_data->'local_id'->>'$[*]' as local_id,
        -- Physical characteristics
        -- json_data->>'weight' as weight,
        json_data->>'edition_name' as edition_name,
        json_data->>'number_of_pages' as number_of_pages,
        json_data->>'pagination' as pagination,
        -- json_data->>'physical_dimensions' as physical_dimensions,
        -- json_data->>'physical_format' as physical_format,
        -- Publication information
        json_data->>'copyright_date' as copyright_date,
        json_data->>'publish_country' as publish_country,
        json_data->>'publish_date' as publish_date,
        json_data->'publish_places'->>'$[*]' as publish_places,
        json_data->'publishers'->>'$[*]' as publishers,
        -- json_data->'contributions'->>'$[*]' as contributions,
        -- Classification and subject information
        json_data->'dewey_decimal_class'->>'$[*]' as dewey_decimal_class,
        json_data->'genres'->>'$[*]' as genres,
        json_data->'lc_classifications'->>'$[*]' as lc_classifications,
        json_data->'other_titles'->>'$[*]' as other_titles,
        json_data->'series'->>'$[*]' as series,
        json_data->'source_records'->>'$[*]' as source_records,
        json_data->'subjects'->>'$[*]' as subjects,
        json_data->'work_titles'->>'$[*]' as work_titles,
        -- -- Language information
        -- json_data->'languages'->>'$[*].key' as languages,
        -- json_data->'translated_from'->>'$[*].key' as translated_from,
        -- json_data->>'translation_of' as translation_of,
        -- Additional metadata
        json_data->>'by_statement' as by_statement,
        json_data->'links'->>'$[*]' as links,
        FROM first_english_edition
) TO 'data/ol_first_editions.parquet' (
    FORMAT parquet,
    COMPRESSION ZSTD,
    COMPRESSION_LEVEL 15
);