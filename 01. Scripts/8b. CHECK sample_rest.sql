-- The following query should give 0 rows as output
-- if the query does give a result, it means that for the rows in that file there are
-- no corresponding samplehashes in sample_core. It means that for a row in tmp_sample_rest
-- its samplehash is different than the samplehas in sample_core
-- This can be the case if in "5. create_database_files_SAMPLE_CORE" a variable gets a different
-- datatype (dtype) than the same variable in "6. create_database_files_SAMPLE_REST" 
-- Changing the datatype so they are identical can solve this issue
SELECT r.file, COUNT(*)
FROM efsa.tmp_sample_rest r
LEFT OUTER JOIN efsa.sample_core c
	ON r.samplehash = c.samplehash
WHERE c.id IS NULL
GROUP BY r.file
ORDER BY file
;