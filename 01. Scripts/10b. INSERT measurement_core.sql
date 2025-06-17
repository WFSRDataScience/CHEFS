BEGIN;
UPDATE efsa.tmp_measurement_core
SET paramcodebaseparam = REPLACE(paramcodebaseparam, 'param=', '')
WHERE SUBSTRING(paramcodebaseparam, 1, 6) = 'param='
;
COMMIT;

-- --------------------------- Import data from tmp_measurement_core to measurement_core
-- --------------------------- only import new measurements
-- --------------------------- only import measurements with a known param (exclude 'not in list' param)
-- --------------------------- insert data into measurement_core partitioned table
INSERT INTO efsa.measurement_core(sample_core_id, resid, filetype, param_id, restyp_id, resunit_id, resval, resloq, evalcode_id, file_id, measurement_nr)
SELECT *
FROM (
	SELECT
		mc.sample_core_id,
		mc.resid,
		f.filetype,
		p.id,
		v.id,
		u.id,
		mc.resval,
		mc.resloq,
		r.id,
		f.id,
		mc.measurement_nr
	FROM efsa.tmp_measurement_core mc
	LEFT OUTER JOIN ontologies_efsa.valtyp v
		ON mc.restype = v.termcode
	LEFT OUTER JOIN ontologies_efsa.unit u
		ON mc.resunit = u.termcode
	LEFT OUTER JOIN ontologies_efsa.reseval r
		ON mc.evalcode = r.termcode	
	LEFT OUTER JOIN ontologies_efsa.param p
		ON REPLACE(mc.paramcodebaseparam, 'param=', '') = p.termcode
	LEFT OUTER JOIN efsa.files f
		ON mc.file_id = f.id
	LEFT OUTER JOIN efsa.measurement_core mc2
		ON mc.sample_core_id = mc2.sample_core_id
		AND p.id = mc2.param_id
		AND mc.resid = mc2.resid
	WHERE p.termcode <> 'rf-xxxx-xxx-xxx'
	AND mc2.id IS NULL
	
	UNION ALL

	SELECT
		mc.sample_core_id,
		mc.resid,
		f.filetype,
		p.id,
		v.id,
		u.id,
		mc.resval,
		mc.resloq,
		r.id,
		f.id,
		mc.measurement_nr
	FROM efsa.tmp_measurement_core mc
	LEFT OUTER JOIN ontologies_efsa.valtyp v
		ON mc.restype = v.termcode
	LEFT OUTER JOIN ontologies_efsa.unit u
		ON mc.resunit = u.termcode
	LEFT OUTER JOIN ontologies_efsa.reseval r
		ON mc.evalcode = r.termcode	
	LEFT OUTER JOIN ontologies_efsa.param p
		ON REPLACE(mc.paramcodebaseparam, 'param=', '') = p.termcode
	LEFT OUTER JOIN efsa.files f
		ON mc.file_id = f.id
	LEFT OUTER JOIN (
		SELECT sample_core_id
		FROM efsa.measurement_core
		GROUP BY sample_core_id
	) AS mc2
		ON mc.sample_core_id = mc2.sample_core_id
	WHERE p.termcode = 'rf-xxxx-xxx-xxx'
	AND mc2.sample_core_id IS NULL
);