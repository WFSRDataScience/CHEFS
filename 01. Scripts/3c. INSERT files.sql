-- --------------------------- Import file data from tmp_files to files
-- --------------------------- only import new files (files not yet in efsa.files)
INSERT INTO efsa.files(filename, filetype, year, country_code, nr_cols)
SELECT
	LOWER(tf.filename) AS filename,
	LOWER(tf.filetype) AS filetype,
	tf.year,
	LOWER(tf.country_code) AS country_code,
	tf.nr_cols
FROM efsa.tmp_files tf
LEFT OUTER JOIN efsa.files f
	ON LOWER(tf.filename) = f.filename
WHERE f.filename IS NULL
;