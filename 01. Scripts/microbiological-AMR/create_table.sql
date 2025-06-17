CREATE TEMPORARY TABLE tmp_amr(
	id SERIAL PRIMARY KEY,
	resultcode varchar not null,
	repyear numeric ,
	zoonosisst numeric ,
	zoonosist numeric ,
	zoonosiscc numeric ,
	zoonosis_id numeric,
	cc numeric ,
	st numeric ,
	t numeric ,
	matrix_id numeric,
	totunitstested numeric ,
	totunitspositive numeric ,
	totsampunitstested numeric ,
	totsampunitspositive numeric ,
	sampunittype_id numeric,
	sampstage_id numeric,
	samporig_id varchar ,
	samptype_id numeric,
	sampcontext_id numeric,
	sampler_id numeric,
	progcode varchar ,
	progcode_code varchar ,
	progsampstrategy_id numeric,
	seqd numeric ,
	labisolcode varchar ,
	seqm numeric ,
	labtotisol numeric ,
	sampy numeric ,
	sampm numeric ,
	sampd numeric ,
	isoly numeric ,
	isolm numeric ,
	isold numeric ,
	analysisy numeric ,
	analysism numeric ,
	analysisd numeric ,
	anmethcode_id numeric,
	substance_id numeric,
	lowest_id numeric,
	highest_id numeric,
	mic_id numeric,
	cutoffvalue numeric ,
	esbl_code varchar ,
	ampc_code varchar ,
	carbapenem_code numeric,
	syntestcaz varchar ,
	syntestctx varchar ,
	syntestfep varchar ,
	percc varchar ,
	permlst varchar ,
	seqtech_code numeric,
	seqy numeric ,
	tracescode_code numeric,
	filename varchar,
	UNIQUE(filename, resultcode)
);


DROP TABLE IF EXISTS efsa.amr;
CREATE TABLE efsa.amr(
	id SERIAL PRIMARY KEY,
	resultcode varchar not null,
	repyear integer ,
	zoonosisst integer ,
	zoonosist integer ,
	zoonosiscc integer ,
	zoonosis_id integer REFERENCES ontologies_efsa.PARAM(id),
	cc integer ,
	st integer ,
	t integer ,
	matrix_id integer REFERENCES ontologies_efsa.ZOO_CAT_MATRIX(id),
	totunitstested integer ,
	totunitspositive integer ,
	totsampunitstested integer ,
	totsampunitspositive integer ,
	sampunittype_id integer REFERENCES ontologies_efsa.SAMPUNTYP(id),
	sampstage_id integer REFERENCES ontologies_efsa.SAMPNT(id),
	samporig_id varchar ,
	samptype_id integer REFERENCES ontologies_efsa.ZOO_CAT_SMPTYP(id),
	sampcontext_id integer REFERENCES ontologies_efsa.PRGTYP(id),
	sampler_id integer REFERENCES ontologies_efsa.SAMPLR(id),
	progcode varchar ,
	progcode_code varchar ,
	progsampstrategy_id integer REFERENCES ontologies_efsa.SAMPSTR(id),
	seqd integer ,
	labisolcode varchar ,
	seqm integer ,
	labtotisol integer ,
	sampy integer ,
	sampm integer ,
	sampd integer ,
	isoly integer ,
	isolm integer ,
	isold integer ,
	analysisy integer ,
	analysism integer ,
	analysisd integer ,
	anmethcode_id integer REFERENCES ontologies_efsa.ANLYMD(id),
	substance_id integer REFERENCES ontologies_efsa.PARAM(id),
	lowest_id integer REFERENCES ontologies_efsa.ZOO_CAT_FIXMEAS(id),
	highest_id integer REFERENCES ontologies_efsa.ZOO_CAT_FIXMEAS(id),
	mic_id integer REFERENCES ontologies_efsa.ZOO_CAT_FIXMEAS(id),
	cutoffvalue integer ,
	esbl_code varchar ,
	ampc_code varchar ,
	carbapenem_code integer REFERENCES ontologies_efsa.PARAM(id),
	syntestcaz varchar ,
	syntestctx varchar ,
	syntestfep varchar ,
	percc varchar ,
	permlst varchar ,
	seqtech_code integer REFERENCES ontologies_efsa.INSTRUM(id),
	seqy integer ,
	tracescode_code integer REFERENCES ontologies_efsa.ZOO_CAT_TRACES(id),
	filename varchar,
	UNIQUE(filename, resultcode)
);

-- insert values
INSERT INTO efsa.amr SELECT * FROM tmp_amr;

-- VIEW
CREATE OR REPLACE VIEW efsa.vw_amr AS
SELECT
	resultcode,
	repyear ,
	zoonosisst ,
	zoonosist ,
	zoonosiscc ,
	zoonosis_id,
	p1.termcode AS zoonosis_code,
	p1.termextendedname AS zoonosis_name,
	cc ,
	st ,
	t ,
	matrix_id,
	zcm1.termcode AS matrix_code,
	zcm1.termextendedname AS matrix_name,
	totunitstested ,
	totunitspositive ,
	totsampunitstested ,
	totsampunitspositive ,
	sampunittype_id,
	st1.termcode AS sampunittype_code,
	st1.termextendedname AS sampunittype_name,
	sampstage_id,
	snt1.termcode AS sampstage_code,
	snt1.termextendedname AS sampstage_name,
	samporig_id ,
	samptype_id,
	smp1.termcode AS samptype_code,
	smp1.termextendedname AS samptype_name,
	sampcontext_id,
	pt1.termcode AS sampcontext_code,
	pt1.termextendedname AS sampcontext_name,
	sampler_id ,
	splr1.termcode AS sampler_code,
	splr1.termextendedname AS sampler_name,
	progcode ,
	progcode_code ,
	progsampstrategy_id,
	sstr1.termcode AS progsampstrategy_code,
	sstr1.termextendedname AS progsampstrategy_name,
	seqd ,
	labisolcode ,
	seqm ,
	labtotisol ,
	sampy ,
	sampm ,
	sampd ,
	isoly ,
	isolm ,
	isold ,
	analysisy ,
	analysism ,
	analysisd ,
	anmethcode_id,
	an1.termcode AS anmethcode_code,
	an1.termextendedname AS anmethcode_name,
	substance_id ,
	p2.termcode AS substance_code,
	p2.termextendedname AS substance_name,
	lowest_id ,
	zcf1.termcode AS lowest_code,
	zcf1.termextendedname AS lowest_name,
	highest_id ,
	zcf2.termcode AS highest_code,
	zcf2.termextendedname AS highest_name,
	mic_id ,
	zcf3.termcode AS mic_code,
	zcf3.termextendedname AS mic_name,
	cutoffvalue ,
	esbl_code ,
	ampc_code ,
	carbapenem_code AS carbapenem_id,
	p3.termcode AS carbapenem_code,
	p3.termextendedname AS carbapenem_name,
	syntestcaz ,
	syntestctx ,
	syntestfep ,
	percc ,
	permlst ,
	seqtech_code AS seqtech_id,
	i1.termcode AS seqtech_code,
	i1.termextendedname AS seqtech_name,
	seqy ,
	tracescode_code AS tracescode_id,
	zct1.termcode AS tracescode_code,
	zct1.termextendedname AS tracescode_name,
	filename
FROM efsa.amr a
LEFT OUTER JOIN ontologies_efsa.param p1
	ON a.zoonosis_id = p1.id
LEFT OUTER JOIN ontologies_efsa.zoo_cat_matrix zcm1
	ON a.matrix_id = zcm1.id
LEFT OUTER JOIN ontologies_efsa.sampuntyp st1
	ON a.sampunittype_id = st1.id
LEFT OUTER JOIN ontologies_efsa.sampnt snt1
	ON a.sampstage_id = snt1.id
LEFT OUTER JOIN ontologies_efsa.zoo_cat_smptyp smp1
	ON a.samptype_id = smp1.id
LEFT OUTER JOIN ontologies_efsa.prgtyp pt1
	ON a.sampcontext_id = pt1.id
LEFT OUTER JOIN ontologies_efsa.samplr splr1
	ON a.sampler_id = splr1.id
LEFT OUTER JOIN ontologies_efsa.sampstr sstr1
	ON a.progsampstrategy_id = sstr1.id
LEFT OUTER JOIN ontologies_efsa.anlymd an1
	ON a.anmethcode_id = an1.id
LEFT OUTER JOIN ontologies_efsa.param p2
	ON a.substance_id = p2.id
LEFT OUTER JOIN ontologies_efsa.zoo_cat_fixmeas zcf1
	ON a.lowest_id = zcf1.id
LEFT OUTER JOIN ontologies_efsa.zoo_cat_fixmeas zcf2
	ON a.highest_id = zcf2.id
LEFT OUTER JOIN ontologies_efsa.zoo_cat_fixmeas zcf3
	ON a.mic_id = zcf3.id
LEFT OUTER JOIN ontologies_efsa.param p3
	ON a.carbapenem_code = p3.id
LEFT OUTER JOIN ontologies_efsa.instrum i1
	ON a.seqtech_code = i1.id
LEFT OUTER JOIN ontologies_efsa.zoo_cat_traces zct1
	ON a.tracescode_code = zct1.id
;