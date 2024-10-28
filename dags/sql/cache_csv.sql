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
    WITH all_time AS (
        SELECT COUNT(*) as total
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
    ),
    last_7_days AS (
        SELECT COUNT(*) as total
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ),
    last_30_days AS (
        SELECT COUNT(*) as total
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ),
    ytd AS (
        SELECT COUNT(*) as total
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
    )

    -- All time
    SELECT 
        keywords.element AS skill,
        COUNT(job_id) / (SELECT total FROM all_time) AS skill_percent,
        COUNT(job_id) AS skill_count,
        (SELECT total FROM all_time) AS total_jobs,
        'All time' as timeframe
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
        UNNEST(keywords_all.list) AS keywords
    GROUP BY skill 

    UNION ALL

    -- Last 7 days
    SELECT 
        keywords.element AS skill,
        COUNT(job_id) / (SELECT total FROM last_7_days) AS skill_percent,
        COUNT(job_id) AS skill_count,
        (SELECT total FROM last_7_days) AS total_jobs,
        'Last 7 days' as timeframe
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
        UNNEST(keywords_all.list) AS keywords
    WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY skill

    UNION ALL

    -- Last 30 days
    SELECT 
        keywords.element AS skill,
        COUNT(job_id) / (SELECT total FROM last_30_days) AS skill_percent,
        COUNT(job_id) AS skill_count,
        (SELECT total FROM last_30_days) AS total_jobs,
        'Last 30 days' as timeframe
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
        UNNEST(keywords_all.list) AS keywords
    WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY skill

    UNION ALL

    -- Year to date
    SELECT 
        keywords.element AS skill,
        COUNT(job_id) / (SELECT total FROM ytd) AS skill_percent,
        COUNT(job_id) AS skill_count,
        (SELECT total FROM ytd) AS total_jobs,
        'YTD' as timeframe
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
        UNNEST(keywords_all.list) AS keywords
    WHERE search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
    GROUP BY skill

    ORDER BY timeframe, skill_count DESC
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

EXPORT DATA
    OPTIONS (
        uri = 'gs://gsearch_share/cache/skills/timeframes/alltime-*.csv',
        format = 'CSV',
        overwrite = true,
        header = true,
        field_delimiter = ',')
AS (
    WITH total_jobs_country_title AS (
        SELECT job_title_final, search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        GROUP BY job_title_final, search_country
    ),
    total_jobs_title AS (
        SELECT job_title_final, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        GROUP BY job_title_final
    ),
    total_jobs_country AS (
        SELECT search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        GROUP BY search_country
    )

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country_title t 
        ON t.job_title_final = j.job_title_final 
        AND t.search_country = j.search_country
    GROUP BY 
        skill,
        j.job_title_final,
        j.search_country,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        NULL AS search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_title t 
        ON t.job_title_final = j.job_title_final
    GROUP BY 
        skill,
        j.job_title_final,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        NULL AS job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country t 
        ON t.search_country = j.search_country
    GROUP BY 
        skill,
        j.search_country,
        t.job_count

    ORDER BY skill_count DESC
);

EXPORT DATA
    OPTIONS (
        uri = 'gs://gsearch_share/cache/skills/timeframes/7day-*.csv',
        format = 'CSV',
        overwrite = true,
        header = true,
        field_delimiter = ',')
AS (
    WITH total_jobs_country_title AS (
        SELECT job_title_final, search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        GROUP BY job_title_final, search_country
    ),
    total_jobs_title AS (
        SELECT job_title_final, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        GROUP BY job_title_final
    ),
    total_jobs_country AS (
        SELECT search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        GROUP BY search_country
    )

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country_title t 
        ON t.job_title_final = j.job_title_final 
        AND t.search_country = j.search_country
    WHERE j.search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY 
        skill,
        j.job_title_final,
        j.search_country,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        NULL AS search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_title t 
        ON t.job_title_final = j.job_title_final
    WHERE j.search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY 
        skill,
        j.job_title_final,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        NULL AS job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country t 
        ON t.search_country = j.search_country
    WHERE j.search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY 
        skill,
        j.search_country,
        t.job_count

    ORDER BY skill_count DESC
);

EXPORT DATA
    OPTIONS (
        uri = 'gs://gsearch_share/cache/skills/timeframes/30day-*.csv',
        format = 'CSV',
        overwrite = true,
        header = true,
        field_delimiter = ',')
AS (
    WITH total_jobs_country_title AS (
        SELECT job_title_final, search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY job_title_final, search_country
    ),
    total_jobs_title AS (
        SELECT job_title_final, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY job_title_final
    ),
    total_jobs_country AS (
        SELECT search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY search_country
    )

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country_title t 
        ON t.job_title_final = j.job_title_final 
        AND t.search_country = j.search_country
    WHERE j.search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 
        skill,
        j.job_title_final,
        j.search_country,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        NULL AS search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_title t 
        ON t.job_title_final = j.job_title_final
    WHERE j.search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 
        skill,
        j.job_title_final,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        NULL AS job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country t 
        ON t.search_country = j.search_country
    WHERE j.search_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 
        skill,
        j.search_country,
        t.job_count

    ORDER BY skill_count DESC
);

EXPORT DATA
    OPTIONS (
        uri = 'gs://gsearch_share/cache/skills/timeframes/ytd-*.csv',
        format = 'CSV',
        overwrite = true,
        header = true,
        field_delimiter = ',')
AS (
    WITH total_jobs_country_title AS (
        SELECT job_title_final, search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
        GROUP BY job_title_final, search_country
    ),
    total_jobs_title AS (
        SELECT job_title_final, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
        GROUP BY job_title_final
    ),
    total_jobs_country AS (
        SELECT search_country, COUNT(*) AS job_count
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
        GROUP BY search_country
    )

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country_title t 
        ON t.job_title_final = j.job_title_final 
        AND t.search_country = j.search_country
    WHERE j.search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
    GROUP BY 
        skill,
        j.job_title_final,
        j.search_country,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        j.job_title_final,
        NULL AS search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_title t 
        ON t.job_title_final = j.job_title_final
    WHERE j.search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
    GROUP BY 
        skill,
        j.job_title_final,
        t.job_count

    UNION ALL

    SELECT 
        keywords.element AS skill,
        NULL AS job_title_final,
        j.search_country,
        COUNT(j.job_id) / t.job_count AS skill_percent,
        COUNT(j.job_id) AS skill_count,
        t.job_count AS total_jobs
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN total_jobs_country t 
        ON t.search_country = j.search_country
    WHERE j.search_time >= DATE_TRUNC(CURRENT_DATE(), YEAR)
    GROUP BY 
        skill,
        j.search_country,
        t.job_count

    ORDER BY skill_count DESC
);

----------------
-- 🕒 Skills Trend Page
-- "Select All"/"Select All" export
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/skill-trend/skill-trend-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    WITH top_skills AS (
        SELECT keywords.element AS skill
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
            UNNEST(keywords_all.list) AS keywords
        WHERE 1 = 1
        GROUP BY skill
        ORDER BY COUNT(*) DESC
        LIMIT 5
    ),

    skill_counts AS (
        SELECT date, skill, SUM(daily_skill_count) AS daily_skill_count
        FROM (
            SELECT DATE(search_time) AS date,
                keywords.element AS skill,
                COUNT(*) as daily_skill_count
            FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide,
                UNNEST(keywords_all.list) AS keywords
            WHERE 1 = 1
            AND keywords.element IN (SELECT skill FROM top_skills)
            GROUP BY date, skill
        )
        GROUP BY date, skill
    ),

    total_jobs AS (
                SELECT COUNT(*)
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
                WHERE 1 = 1
            ),

    total_jobs_grouped AS (
        SELECT date, SUM(daily_total_count) OVER (
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) as rolling_total_count
        FROM (
            SELECT DATE(search_time) AS date, COUNT(*) AS daily_total_count
            FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
            WHERE 1 = 1
            GROUP BY date
        )
    )

    SELECT sc.date,
        sc.skill,
        (SUM(sc.daily_skill_count) OVER (
            PARTITION BY sc.skill
            ORDER BY sc.date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) / tjg.rolling_total_count) as skill_percentage,
        (SELECT * FROM total_jobs) AS total_jobs
    FROM skill_counts sc
    JOIN total_jobs_grouped tjg ON sc.date = tjg.date
    ORDER BY sc.date DESC, skill_percentage DESC
);

----------------
-- 💰 Skill-Pay Page 
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

-- Selection export
EXPORT DATA
  OPTIONS (
    uri = 'gs://gsearch_share/cache/skill-pay/skill-pay-all-*.csv',
    format = 'CSV',
    overwrite = true,
    header = true,
    field_delimiter = ',')
AS (
    WITH numbered_jobs AS (
        SELECT DISTINCT job_id,
            ROW_NUMBER() OVER (ORDER BY job_id) as job_number
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
    )

    SELECT
        n.job_number,
        j.job_title_final,
        j.search_country,
        j.search_time,
        keywords.element AS skill,
        APPROX_QUANTILES(j.salary_year, 2)[OFFSET(1)] AS median
    FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide j,
        UNNEST(keywords_all.list) AS keywords
    JOIN numbered_jobs n ON j.job_id = n.job_id
    WHERE j.salary_year IS NOT NULL
    GROUP BY n.job_number, j.job_title_final, j.search_country, j.search_time, skill, j.job_id
    ORDER BY j.search_time DESC
);

----------------
-- 💸 Job Salaries Page
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
