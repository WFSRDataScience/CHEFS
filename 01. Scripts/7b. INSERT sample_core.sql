-- --------------------------- Import data from tmp_sample_core to sample_core
-- --------------------------- only import new samples
INSERT INTO efsa.sample_core(samplehash, sampid, sampanid, progid, proglegalref, prgtyp_id, labacc_id, labcountry_id, labsubsampcode, sampmd_id, sampstr_id, anportseq, unit_id, sampnt_id, sampcountry_id, origcountry_id, sampy, sampm, sampd, product_id, analysisy, analysism, analysisd, repyear, file_id) 
SELECT
	
	sc.samplehash,
	sc.sampid,
	sc.sampanid,

	sc.progid,
	sc.proglegalref,
	p.id,

	l.id,
	lc.id,
	sc.labsubsampcode,
	
	smd.id,
	sstr.id,

	sc.anportseq,
	u.id,
	
	snt.id,
	sampc.id,
	origc.id,

	sc.sampy,
	sc.sampm,	
	sc.sampd,

	CASE
	WHEN f.id IS NOT NULL THEN pc.id
	WHEN m.id IS NOT NULL THEN pc2.id
	END AS product_id,

	sc.analysisy,
	sc.analysism,
	sc.analysisd,

	sc.repyear,
	
	fi.id

FROM efsa.tmp_sample_core sc
LEFT OUTER JOIN ontologies_efsa.prgtyp p
	ON sc.progtype = p.termcode
LEFT OUTER JOIN ontologies_efsa.labacc l
	ON sc.labaccred = l.termcode
LEFT OUTER JOIN ontologies_efsa.country lc
	ON sc.labcountry = lc.termcode	
LEFT OUTER JOIN ontologies_efsa.sampmd smd
	ON sc.sampmethod = smd.termcode	
LEFT OUTER JOIN ontologies_efsa.sampstr sstr
	ON sc.sampstrategy = sstr.termcode	
LEFT OUTER JOIN ontologies_efsa.unit u
	ON sc.anportsizeunit = u.termcode	
LEFT OUTER JOIN ontologies_efsa.sampnt snt
	ON sc.samppoint = snt.termcode
LEFT OUTER JOIN ontologies_efsa.country sampc
	ON sc.sampcountry = sampc.termcode	
LEFT OUTER JOIN ontologies_efsa.country origc
	ON sc.origcountry = origc.termcode
LEFT OUTER JOIN ontologies_efsa.mtx f
	ON sc.sampmatcodebasebuilding = f.termcode	
LEFT OUTER JOIN ontologies_efsa.product_catalogue pc
	ON f.id = pc.mtx_id
LEFT OUTER JOIN ontologies_efsa.matrix m
	ON sc.sampmatcodebasebuilding = m.termcode	
LEFT OUTER JOIN ontologies_efsa.product_catalogue pc2
	ON m.id = pc2.matrix_id
LEFT OUTER JOIN efsa.files fi
	ON LOWER(sc.file) = LOWER(fi.filename)
LEFT OUTER JOIN efsa.sample_core s
	ON sc.samplehash = s.samplehash
WHERE s.samplehash IS NULL
;