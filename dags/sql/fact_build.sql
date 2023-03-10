-- pre-JSON table create
-- create table in format needed to combine with JSON data; only need to run once
-- BKGD: 'gsearch_job_listings.gsearch_jobs_all' was first used to collect data before collecting full JSON
CREATE OR REPLACE TABLE `job-listings-366015`.gsearch_job_listings.gsearch_jobs_all_json_version AS
    (SELECT title          AS job_title,
            company_name,
            location       AS job_location,
            via            AS job_via,
            description    AS job_description,
            extensions     as job_extensions,
            job_id,
            thumbnail      AS company_thumbnail,
            posted_at      AS job_posted_at,
            schedule_type  AS job_schedule_type,
            work_from_home AS job_work_from_home,
            salary         AS job_salary,
            search_term,
            search_location,
            date_time      AS search_time,
            commute_time   AS job_commute_time,
     FROM `job-listings-366015`.gsearch_job_listings.gsearch_jobs_all
-- started collecting data in 'gsearch_jobs_all_json' on 1-1-2023 (142,416 records)
-- No longer need to use data from this 'back-up' table after this period
     WHERE date_time < (SELECT MIN(search_time)
                        FROM `job-listings-366015`.gsearch_job_listings_json.gsearch_jobs_all_json));

-- JSON table create
-- 43 second run-time (175K rows) on 05Jan23, 4 mins (450K rows) on 09Mar2023
CREATE OR REPLACE TABLE `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_fact AS
    (WITH gsearch_json_all AS
              -- Extract JSON data and combine with pre-JSON data
              (SELECT JSON_EXTRACT_SCALAR(jobs_results.job_id)                            AS job_id,
                      JSON_EXTRACT_SCALAR(jobs_results.title)                             AS job_title,
                      JSON_EXTRACT_SCALAR(jobs_results.company_name)                      AS company_name,
                      JSON_EXTRACT_SCALAR(jobs_results.location)                          AS job_location,
                      JSON_EXTRACT_SCALAR(jobs_results.via)                               AS job_via,
                      JSON_EXTRACT_SCALAR(jobs_results.description)                       AS job_description,
                      -- how to get all 'job_highlights'
                      -- JSON_EXTRACT(jobs_results.job_highlights, '$')                       AS job_highlights_all,
                      CASE
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[0].title')) =
                               'Qualifications'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[0].items')
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[1].title')) =
                               'Qualifications'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[1].items')
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[2].title')) =
                               'Qualifications'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[2].items')
                          END                                                             AS job_highlights_qualifications,
                      CASE

                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[0].title')) =
                               'Responsibilities'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[0].items')
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[1].title')) =
                               'Responsibilities'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[1].items')
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[2].title')) =
                               'Responsibilities'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[2].items')
                          END                                                             AS job_highlights_responsibilities,
                      CASE
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[0].title')) =
                               'Benefits'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[0].items')
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[1].title')) =
                               'Benefits'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[1].items')
                          WHEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.job_highlights, '$[2].title')) =
                               'Benefits'
                              THEN JSON_EXTRACT(jobs_results.job_highlights, '$[2].items')
                          END                                                             AS job_highlights_benefits,
                      JSON_EXTRACT_SCALAR(jobs_results.detected_extensions.posted_at)     AS job_posted_at,
                      JSON_EXTRACT_SCALAR(jobs_results.detected_extensions.salary)        AS job_salary,
                      JSON_EXTRACT_SCALAR(jobs_results.detected_extensions.schedule_type) AS job_schedule_type,
                      CAST(JSON_EXTRACT_SCALAR(
                              jobs_results.detected_extensions.work_from_home) AS BOOL)   AS job_work_from_home,
                      JSON_EXTRACT_SCALAR(jobs_results.detected_extensions.commute_time)  AS job_commute_time,
                      JSON_VALUE_ARRAY(jobs_results, '$.extensions')                      AS job_extensions,
                      CASE
                          WHEN LEFT(
                                       JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.related_links, '$[0].link')),
                                       22) !=
                               'https://www.google.com'
                              THEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.related_links, '$[0].link'))
                          END
                                                                                          AS company_link,
                      CASE
                          WHEN LEFT(
                                       JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.related_links, '$[0].text')),
                                       7) = 'See web'
                              THEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.related_links, '$[0].link'))
                          WHEN LEFT(
                                       JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.related_links, '$[1].text')),
                                       7) = 'See web'
                              THEN JSON_EXTRACT_SCALAR(JSON_EXTRACT(jobs_results.related_links, '$[1].link'))
                          END
                                                                                          AS company_link_google,
                      JSON_EXTRACT_SCALAR(jobs_results.thumbnail)                         AS company_thumbnail,
                      error,
                      search_term,
                      search_location,
                      search_time,
                      search_id,
               FROM `job-listings-366015`.gsearch_job_listings_json.gsearch_jobs_all_json
                        -- Used to unpack original JSON
                        LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(results.jobs_results)) AS jobs_results
               WHERE error = false
-- Union with previous data not previously saved as JSON
               UNION ALL
               SELECT job_id,
                      job_title,
                      company_name,
                      job_location,
                      job_via,
                      job_description,
                      null as job_highlights_qualifications,
                      null as job_highlights_responsibilities,
                      null as job_highlights_benefits,
                      job_posted_at,
                      job_salary,
                      job_schedule_type,
                      job_work_from_home,
                      job_commute_time,
                      job_extensions,
                      null as company_link,
                      null as company_link_google,
                      company_thumbnail,
                      null as error,
                      search_term,
                      search_location,
                      search_time,
                      null as search_id,
               FROM `job-listings-366015`.gsearch_job_listings.gsearch_jobs_all_json_version)
-- clean table once combined for those with common fields
     SELECT *,
            CASE
                WHEN "No degree mentioned" IN UNNEST(job_extensions)
                    THEN true
                END AS job_no_degree_mention,
            CASE
                WHEN "Health insurance" IN UNNEST(job_extensions)
                    THEN true
                END AS job_health_insurance,
     FROM gsearch_json_all);
