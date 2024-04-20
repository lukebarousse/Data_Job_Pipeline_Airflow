-- Public table build for Langchain interaction
-- 8 second run-tim (1.2M rows) on 5APR24
-- Does some minor cleanup like:
-- 1. Removing duplicates based on PARTITION BY company_name, job_title, job_schedule_type, job_description, job_location
-- 2. removing the "via" from the job_via column (i did this in the course so want to be consistent)
-- 3. removing the job_title_clean column (as it may be blank for some entries); job_title_final is the one to use
-- 4. converting the keywords_all.list array to a simple array
-- 5. converting the job_work_from_home, job_no_degree_mention, job_health_insurance to boolean (fill in na as false4)
-- columns not used: job_via, job_title_clean, job_description, job_posted_at, job_salary, job_commute_time, company_link, company_link_google, company_thumbnail, job_highlights_qualifications, job_highlights_responsibilities, job_highlights_benefits, job_extensions, error, search_id, search_term, search_location, keywords_programming, keywords_databases, keywords_cloud, keywords_libraries, keywords_webframeworks, keywords_os, keywords_analyst_tools, keywords_other, keywords_async, keywords_sync, salary_pay, salary_avg, salary_min, salary_max

CREATE OR REPLACE TABLE `job-listings-366015`.public_job_listings.data_nerd_jobs AS
SELECT
  job_title_final,
  job_title                             AS job_title_original,
  company_name,
  job_location,
  search_time                           AS job_posted_at,
  REPLACE(job_via, 'via ', '')          AS job_posting_site,
  job_schedule_type,
  IFNULL(job_work_from_home, FALSE)     AS job_work_from_home,
  IFNULL(job_no_degree_mention, FALSE)  AS job_no_degree_mention,
  IFNULL(job_health_insurance, FALSE)   AS job_health_insurance,
  ARRAY(
    SELECT x.element
    FROM UNNEST(keywords_all.list) AS x
  )                                     AS job_keywords
FROM (
  SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY company_name, job_title, job_schedule_type, job_description, job_location) AS rn
  FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
)
WHERE rn = 1;