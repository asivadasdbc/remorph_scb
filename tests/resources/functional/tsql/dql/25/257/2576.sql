--Query type: DQL
SELECT DISTINCT job_title FROM (VALUES ('Sales Representative'), ('Sales Representative'), ('Sales Manager'), ('Sales Manager'), ('Marketing Manager')) AS employee(job_title) ORDER BY job_title;