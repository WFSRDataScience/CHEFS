-- --------------------------- import data from tmp_sample_rest to sample_rest
-- --------------------------- only import new samples
INSERT INTO efsa.sample_rest(sample_core_id, column_metainfo_id, value)
SELECT
	c.id,
	cm.id,
	r.value

FROM efsa.tmp_sample_rest r
JOIN efsa.sample_core c
	ON r.samplehash = c.samplehash
JOIN efsa.column_metainfo cm
	ON r.variable = cm.columnname
LEFT OUTER JOIN (
	SELECT c1.samplehash
	FROM efsa.sample_rest r1
	JOIN efsa.sample_core c1
		ON r1.sample_core_id = c1.id
) AS t1
	ON t1.samplehash = r.samplehash
WHERE t1.samplehash IS NULL
;