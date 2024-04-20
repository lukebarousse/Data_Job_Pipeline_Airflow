-- Final wide table build that combines fact and dimension tables
-- 77 second run-time (550K rows) on 09Mar23

CREATE OR REPLACE TABLE `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide AS
        (SELECT  
                CASE 
                        WHEN job_title_clean IS NULL THEN search_term
                        ELSE job_title_clean
                END AS job_title_final,
                t.* EXCEPT (job_title),
                f.*,
                s.* EXCEPT (job_id, job_description),
                c.* EXCEPT (search_location),
                d.* EXCEPT (job_id)
        FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_fact AS f
                LEFT JOIN `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills AS s ON f.job_id = s.job_id 
                LEFT JOIN `job-listings-366015`.gsearch_job_listings_clean.gsearch_country AS c ON c.search_location = f.search_location
                LEFT JOIN `job-listings-366015`.gsearch_job_listings_clean.gsearch_salary AS d ON d.job_id = f.job_id
                LEFT JOIN `job-listings-366015`.gsearch_job_listings_clean.gsearch_job_title AS t ON t.job_title = f.job_title
        );

-- Keywords table
-- Had to create a physical table as unable to figure out how to export this query to one CSV for cache of website
CREATE OR REPLACE TABLE `job-listings-366015`.gsearch_job_listings_clean.gsearch_keywords AS
        (WITH keywords AS (
        SELECT DISTINCT keywords_all AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_all.element AS keywords_all
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_all.list) AS keywords_all
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_programming AS (
        SELECT DISTINCT keywords_programming AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_programming.element AS keywords_programming
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_programming.list) AS keywords_programming
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_databases AS (
        SELECT DISTINCT keywords_databases AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_databases.element AS keywords_databases
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_databases.list) AS keywords_databases
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_cloud AS (
        SELECT DISTINCT keywords_cloud AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_cloud.element AS keywords_cloud
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_cloud.list) AS keywords_cloud
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_libraries AS (
        SELECT DISTINCT keywords_libraries AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_libraries.element AS keywords_libraries
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_libraries.list) AS keywords_libraries
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_webframeworks AS (
        SELECT DISTINCT keywords_webframeworks AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_webframeworks.element AS keywords_webframeworks
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_webframeworks.list) AS keywords_webframeworks
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_os AS (
        SELECT DISTINCT keywords_os AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_os.element AS keywords_os
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_os.list) AS keywords_os
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_analyst_tools AS (
        SELECT DISTINCT keywords_analyst_tools AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_analyst_tools.element AS keywords_analyst_tools
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_analyst_tools.list) AS keywords_analyst_tools
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_other AS (
        SELECT DISTINCT keywords_other AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_other.element AS keywords_other
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_other.list) AS keywords_other
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        ), keywords_async AS (
        SELECT DISTINCT keywords_async AS element,
                SPLIT(kv, ':')[OFFSET(0)] as keyword,
        FROM (
                SELECT DISTINCT keywords_async.element AS keywords_async
                FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_skills,
                UNNEST(keywords_async.list) AS keywords_async
        ) AS k,
                UNNEST(SPLIT(TRANSLATE(TO_JSON_STRING(k), '"{}', ''))) kv
        )

        SELECT * FROM keywords
        UNION ALL
        SELECT * FROM keywords_programming
        UNION ALL
        SELECT * FROM keywords_databases
        UNION ALL
        SELECT * FROM keywords_cloud
        UNION ALL 
        SELECT * FROM keywords_libraries
        UNION ALL
        SELECT * FROM keywords_webframeworks
        UNION ALL
        SELECT * FROM keywords_os
        UNION ALL
        SELECT * FROM keywords_analyst_tools
        UNION ALL
        SELECT * FROM keywords_other
        UNION ALL
        SELECT * FROM keywords_async
);


-- Salary table for salary page
-- Had to create a physical table as unable to figure out how to export this query to one CSV for cache of website
CREATE OR REPLACE TABLE `job-listings-366015`.gsearch_job_listings_clean.gsearch_salary_wide AS
        (SELECT
            job_title_final AS job_title,
            search_term,
            salary_avg,
            salary_min,
            salary_max,
            salary_year,
            salary_hour,
            search_location,
            job_location,
            job_schedule_type,
            job_via,
            search_country,
            search_time,
        FROM
            `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
        WHERE salary_avg IS NOT NULL
        );