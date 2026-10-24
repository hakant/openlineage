-- Example queries against the Marquez database.
--
-- Run all of them:
--   docker exec -i marquez-db psql -U marquez -d marquez < stage4_explore/queries.sql
-- or open an interactive shell:
--   docker exec -it marquez-db psql -U marquez -d marquez
-- (the DB is also published on localhost:5432, user/password/db: marquez)
--
-- Key tables
--   lineage_events    raw events exactly as received (jsonb) - the source of truth
--   jobs, job_versions, job_versions_io_mapping   jobs and their current inputs/outputs
--   runs, run_states, run_facets                  run history and run-level facets
--   datasets, dataset_versions, dataset_fields    datasets, versions (one per writing run), columns
--   dataset_facets                                dataset facets per version
--   column_lineage                                output field <- input field edges

\echo '\n== 1. Events received, by producer and event type'
SELECT producer, event_type, count(*) AS events
FROM lineage_events
GROUP BY producer, event_type
ORDER BY producer, event_type;

\echo '\n== 2. Run counts per job and state'
SELECT namespace_name, job_name, current_run_state, count(*) AS runs
FROM runs
GROUP BY namespace_name, job_name, current_run_state
ORDER BY namespace_name, job_name, current_run_state;

\echo '\n== 3. Failed runs with their error message (ErrorMessageRunFacet)'
SELECT r.namespace_name,
       r.job_name,
       r.ended_at,
       rf.facet -> 'errorMessage' ->> 'message' AS error_message
FROM runs r
JOIN run_facets rf ON rf.run_uuid = r.uuid AND rf.name = 'errorMessage'
WHERE r.current_run_state = 'FAILED'
ORDER BY r.ended_at DESC;

\echo '\n== 4. Run durations (completed runs), slowest first'
SELECT namespace_name,
       job_name,
       round(extract(epoch FROM (ended_at - started_at))::numeric, 2) AS duration_s,
       started_at
FROM runs
WHERE current_run_state = 'COMPLETED' AND started_at IS NOT NULL AND ended_at IS NOT NULL
ORDER BY duration_s DESC
LIMIT 15;

\echo '\n== 5. Child runs of each parent run (ParentRunFacet)'
SELECT p.job_name AS parent_job,
       p.current_run_state AS parent_state,
       c.job_name AS child_job,
       c.current_run_state AS child_state
FROM runs c
JOIN runs p ON p.uuid = c.parent_run_uuid
ORDER BY p.created_at DESC, c.created_at;

\echo '\n== 6. Current inputs and outputs of every job'
SELECT j.namespace_name,
       j.name AS job,
       m.io_type,
       d.namespace_name || ' :: ' || d.name AS dataset
FROM job_versions_io_mapping m
JOIN jobs j ON j.uuid = m.job_uuid
JOIN datasets d ON d.uuid = m.dataset_uuid
WHERE m.is_current_job_version
ORDER BY j.namespace_name, j.name, m.io_type, dataset;

\echo '\n== 7. Dataset versions and the run that produced them'
SELECT dv.dataset_name,
       dv.created_at,
       r.job_name AS produced_by,
       jsonb_array_length(coalesce(dv.fields, '[]'::jsonb)) AS n_fields
FROM dataset_versions dv
LEFT JOIN runs r ON r.uuid = dv.run_uuid
ORDER BY dv.dataset_name, dv.created_at DESC;

\echo '\n== 8. Latest schema of every dataset (newest version that has one; failed runs create empty versions)'
WITH latest AS (
    SELECT DISTINCT ON (dataset_uuid) dataset_uuid, namespace_name, dataset_name, fields
    FROM dataset_versions
    WHERE jsonb_array_length(coalesce(fields, '[]'::jsonb)) > 0
    ORDER BY dataset_uuid, created_at DESC
)
SELECT namespace_name,
       dataset_name AS dataset,
       string_agg(f ->> 'name' || ' ' || coalesce(nullif(f ->> 'type', ''), '?'), ', ' ORDER BY f ->> 'name') AS columns
FROM latest
CROSS JOIN LATERAL jsonb_array_elements(fields) AS f
GROUP BY namespace_name, dataset_name
ORDER BY namespace_name, dataset_name;

\echo '\n== 9. Schema changes: columns that are not present in every version that has a schema'
WITH versions AS (
    SELECT dataset_uuid, count(*) AS n
    FROM dataset_versions
    WHERE jsonb_array_length(coalesce(fields, '[]'::jsonb)) > 0
    GROUP BY dataset_uuid
),
field_counts AS (
    SELECT dv.dataset_uuid, f ->> 'name' AS field, count(DISTINCT dv.uuid) AS in_versions
    FROM dataset_versions dv
    CROSS JOIN LATERAL jsonb_array_elements(coalesce(dv.fields, '[]'::jsonb)) AS f
    GROUP BY dv.dataset_uuid, f ->> 'name'
)
SELECT d.name AS dataset, fc.field, fc.in_versions, v.n AS total_versions
FROM field_counts fc
JOIN versions v USING (dataset_uuid)
JOIN datasets d ON d.uuid = fc.dataset_uuid
WHERE fc.in_versions < v.n
ORDER BY d.name, fc.field;

\echo '\n== 10. Column lineage edges (input column -> output column)'
SELECT idv.dataset_name || '.' || idf.name AS input_column,
       odv.dataset_name || '.' || odf.name AS output_column,
       cl.transformation_type
FROM column_lineage cl
JOIN dataset_versions idv ON idv.uuid = cl.input_dataset_version_uuid
JOIN dataset_fields idf ON idf.uuid = cl.input_dataset_field_uuid
JOIN dataset_versions odv ON odv.uuid = cl.output_dataset_version_uuid
JOIN dataset_fields odf ON odf.uuid = cl.output_dataset_field_uuid
GROUP BY 1, 2, 3
ORDER BY 2, 1;

\echo '\n== 11. dbt data quality assertions per dataset (dataQualityAssertions facet)'
SELECT DISTINCT ON (d.name, a ->> 'column', a ->> 'assertion')
       d.name AS dataset,
       a ->> 'column' AS column,
       a ->> 'assertion' AS assertion,
       a ->> 'success' AS success
FROM dataset_facets df
JOIN datasets d ON d.uuid = df.dataset_uuid
CROSS JOIN LATERAL jsonb_array_elements(df.facet -> 'dataQualityAssertions' -> 'assertions') AS a
WHERE df.name = 'dataQualityAssertions'
ORDER BY d.name, a ->> 'column', a ->> 'assertion', df.lineage_event_time DESC;

\echo '\n== 12. Row counts reported via the outputStatistics facet'
SELECT d.name AS dataset,
       df.lineage_event_time,
       df.facet -> 'outputStatistics' ->> 'rowCount' AS row_count
FROM dataset_facets df
JOIN datasets d ON d.uuid = df.dataset_uuid
WHERE df.name = 'outputStatistics'
ORDER BY df.lineage_event_time DESC
LIMIT 10;

\echo '\n== 13. Datasets nothing reads from (potential dead ends)'
SELECT d.namespace_name, d.name
FROM datasets d
WHERE NOT EXISTS (
    SELECT 1 FROM job_versions_io_mapping m
    WHERE m.dataset_uuid = d.uuid AND m.io_type = 'INPUT' AND m.is_current_job_version
)
AND EXISTS (SELECT 1 FROM dataset_versions dv WHERE dv.dataset_uuid = d.uuid)
ORDER BY d.namespace_name, d.name;
