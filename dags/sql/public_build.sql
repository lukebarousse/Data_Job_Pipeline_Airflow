-- Public table build for my ChatGPT course
-- 28 second run-tim (680K rows) on 25Oct23
-- Does some minor cleanup like:
-- 1. Remmoving duplicates based on PARTITION BY company_name, job_title, job_schedule_type, job_description, job_location
-- 2. removing the "via" from the job_via column (i did this in the course so want to be consistent)
-- 3. removing the job_title_clean column (as it may be blank for some entries); job_title_final is the one to use


CREATE OR REPLACE TABLE `job-listings-366015`.public_job_listings.data_nerd_jobs AS
SELECT * EXCEPT(rn, job_via, job_title_clean),
  REPLACE(job_via, 'via ', '') AS job_via
FROM (
  SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY company_name, job_title, job_schedule_type, job_description, job_location) AS rn
  FROM `job-listings-366015`.gsearch_job_listings_clean.gsearch_jobs_wide
)
WHERE rn = 1;