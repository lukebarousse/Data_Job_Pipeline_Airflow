----------------
-- 🛠️ Skill Page 
-- "Select All"/"Select All" export
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/skills/skills-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    WITH total_jobs AS (
        SELECT COUNT(*)
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
    )

    SELECT keywords.element                                AS skill,
        COUNT(job_id) / (SELECT * FROM total_jobs)       AS skill_percent,
        COUNT(job_id)                                    AS skill_count,
        (SELECT * FROM total_jobs)                         AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
        UNNEST(keywords_all.list) AS keywords
    GROUP BY skill 
    ORDER BY skill_count DESC
);

-- Slicer
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/skills/slicer-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    SELECT
        job_title_final AS job_title,
        search_country,
        COUNT(*) AS job_count
    FROM
        `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
    WHERE search_country IS NOT NULL
    GROUP BY job_title, search_country
    ORDER BY job_count DESC
);

-- Keywords
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/skills/keywords-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    SELECT * FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_keywords
);

----------------
-- 🆚 Skill-Pay Page 
-- "Select All"/"Select All" export
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/skill-pay/skill-pay-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    WITH total_jobs AS (
        SELECT COUNT(*)
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        -- {job_choice_query} AND salary_year IS NOT NULL
        WHERE salary_year IS NOT NULL
    )

    SELECT
        keywords.element AS skill,
        AVG(salary_year) AS avg,
        MIN(salary_year) AS min,
        MAX(salary_year) AS max,
        APPROX_QUANTILES(salary_year,2)[OFFSET(1)] AS median,
        COUNT(job_id) AS count,
        (SELECT * FROM total_jobs) AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
        UNNEST(keywords_all.list) AS keywords
    -- {job_choice_query} AND salary_year IS NOT NULL
    WHERE salary_year IS NOT NULL
    GROUP BY skill
    ORDER BY count DESC
);

-- Slicer
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/skill-pay/slicer-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    SELECT
        job_title_final AS job_title,
        search_country,
        COUNT(*) AS job_count
    FROM
        `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
    WHERE search_country IS NOT NULL AND salary_year IS NOT NULL
    GROUP BY job_title, search_country
    ORDER BY job_count DESC
);

----------------
-- 💸 Salary Page
-- (No Selections) export
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/salary/salary-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    SELECT * FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_salary_wide
);

----------------
-- 🏥 Health Page exports
-- Calculate num_jobs
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/health/num-jobs-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    SELECT COUNT(*) AS num_jobs,
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_fact
);  

-- Caluclate dates and missing dates
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/health/dates-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    SELECT DISTINCT CAST(search_time AS DATE) AS search_date,
                    COUNT(job_id) AS jobs_daily
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_fact
    GROUP BY search_date
    ORDER BY search_date
);

-- Find last update
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/health/last-update-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    SELECT MAX(search_time) as last_update
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_fact
);
