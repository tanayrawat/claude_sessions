-- Gold = business-facing marts, fully rebuilt from silver (cheap here; incremental/dbt models at scale).

CREATE OR REPLACE TABLE gold.daily_issue_activity AS
WITH opened AS (
    SELECT repo, CAST(created_at AS DATE) AS day, count(*) AS n
    FROM silver.issues WHERE NOT is_pull_request GROUP BY ALL
), closed AS (
    SELECT repo, CAST(closed_at AS DATE) AS day, count(*) AS n
    FROM silver.issues WHERE NOT is_pull_request AND closed_at IS NOT NULL GROUP BY ALL
)
SELECT coalesce(o.repo, c.repo) AS repo,
       coalesce(o.day, c.day)   AS day,
       coalesce(o.n, 0)         AS opened,
       coalesce(c.n, 0)         AS closed
FROM opened o FULL OUTER JOIN closed c ON o.repo = c.repo AND o.day = c.day;

CREATE OR REPLACE TABLE gold.label_resolution AS
SELECT repo,
       label,
       count(*)                                              AS issues,
       count(*) FILTER (WHERE state = 'closed')              AS closed_issues,
       round(median(date_diff('hour', created_at, closed_at)), 1) AS median_hours_to_close
FROM (
    SELECT repo, state, created_at, closed_at, unnest(labels) AS label
    FROM silver.issues WHERE NOT is_pull_request
)
GROUP BY ALL;

CREATE OR REPLACE TABLE gold.open_backlog AS
SELECT repo,
       count(*)                                         AS open_issues,
       count(*) FILTER (WHERE comments = 0)             AS open_without_response,
       max(date_diff('day', created_at, current_timestamp::TIMESTAMP)) AS oldest_open_days
FROM silver.issues WHERE NOT is_pull_request AND state = 'open' GROUP BY ALL;
