DROP TABLE IF EXISTS efsa.column_metainfo;
CREATE TABLE IF NOT EXISTS efsa.column_metainfo
(
    id SERIAL PRIMARY KEY,
    columnname character varying COLLATE pg_catalog."default" NOT NULL,
    descr_sdd2 character varying COLLATE pg_catalog."default",
    descr_sdd1 character varying COLLATE pg_catalog."default",
    sample_measurement character varying COLLATE pg_catalog."default" NOT NULL,
    datatype character varying COLLATE pg_catalog."default" NOT NULL,
    catalogue character varying COLLATE pg_catalog."default",
    CONSTRAINT column_metainfo_columnname_key UNIQUE (columnname),
    CONSTRAINT column_metainfo_sample_measurement_check CHECK (sample_measurement::text = ANY (ARRAY['s'::character varying::text, 'm'::character varying::text]))
);

-- ----------------------------------------------------------------- FILES
-- Table to store information on the raw data files
DROP TABLE IF EXISTS efsa.tmp_files;
CREATE TABLE efsa.tmp_files (
	filename VARCHAR NOT NULL UNIQUE,
	filetype VARCHAR NOT NULL,
	year INT NOT NULL,
	country_code VARCHAR NOT NULL,
	nr_cols INTEGER NOT NULL,
	nr_rows INTEGER NOT NULL
);

CREATE TABLE efsa.files (
	id SERIAL PRIMARY KEY,
	filename VARCHAR NOT NULL UNIQUE,
	filetype VARCHAR NOT NULL,
	year INT NOT NULL,
	country_code VARCHAR NOT NULL,
	nr_cols INTEGER NOT NULL
);


-- ----------------------------------------------------------------- SAMPLE CORE TABLESPACE
-- ------------ a tmp_sample_core is created to store the processed sample core data coming from python
-- ------------ an insert query (see EFSA - INSERTS.sql) then moves data from tmp_sample_core to sample_core
-- ------------ sample_core table is created below
-- ------------ finally some views are created

-- Create a hash function to convert the unique identifier columns to a single unique hash value
CREATE OR REPLACE FUNCTION efsa.samplehash(col1 text, col2 text, col3 text, col4 text, col5 text, col6 text, col7 text, col8 text, col9 text, col10 text, col11 text, col12 text, col13 text, col14 text, col15 text, col16 text)
  RETURNS uuid 
  LANGUAGE sql IMMUTABLE COST 25 PARALLEL SAFE AS 
'SELECT md5(hashtextextended(textin(record_out(($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16))), 0) :: text) :: uuid';


DROP TABLE IF EXISTS efsa.tmp_sample_core;
CREATE TABLE efsa.tmp_sample_core (
	id SERIAL PRIMARY KEY,
	analysisd NUMERIC,
	analysism NUMERIC,
	analysisy NUMERIC NOT NULL,
	anportseq NUMERIC,
	anportsizeunit VARCHAR,
	file VARCHAR NOT NULL,
	labaccred VARCHAR,
	labcountry VARCHAR,
	labsubsampcode VARCHAR,
	origcountry VARCHAR,
	progid VARCHAR,
	proglegalref VARCHAR,
	progtype VARCHAR,
	repyear NUMERIC,
	sampanid VARCHAR,
	sampcountry VARCHAR NOT NULL,
	sampd NUMERIC,
	sampid VARCHAR NOT NULL,
	sampm NUMERIC,
	sampmatcodebasebuilding VARCHAR,
	sampmethod VARCHAR,
	samppoint VARCHAR,
	sampstrategy VARCHAR,
	sampy INTEGER NOT NULL,
	samplehash uuid NOT NULL UNIQUE GENERATED ALWAYS AS (efsa.samplehash(	
																	sampid::text, 
																	sampanid::text,
																	progid::text,
																	proglegalref::text,
																	labaccred::text,
																	labcountry::text,
																	labsubsampcode::text,
																	anportseq::text,
																	anportsizeunit::text,
																	sampcountry::text,
																	analysisy::text,
																	analysism::text,
																	analysisd::text,
																	repyear::text,
																	sampmatcodebasebuilding::text,
																	file::text)) STORED
);

-- sample_core table
--DROP TABLE IF EXISTS efsa.sample_core;
CREATE TABLE efsa.sample_core (
	id SERIAL PRIMARY KEY,
	
	samplehash uuid NOT NULL UNIQUE,
	
	sampid VARCHAR NOT NULL,
	sampanid VARCHAR,

	progid VARCHAR,
	proglegalref VARCHAR,
	prgtyp_id INTEGER NOT NULL REFERENCES ontologies_efsa.prgtyp(id),

	labacc_id INTEGER REFERENCES ontologies_efsa.labacc(id),
	labcountry_id INTEGER REFERENCES ontologies_efsa.country(id),
	labsubsampcode VARCHAR,
	
	sampmd_id INTEGER REFERENCES ontologies_efsa.sampmd(id),
	sampstr_id INTEGER NOT NULL REFERENCES ontologies_efsa.sampstr(id),

	anportseq INTEGER,
	unit_id INTEGER REFERENCES ontologies_efsa.unit(id),
	
	sampnt_id INTEGER NOT NULL REFERENCES ontologies_efsa.sampnt(id),
	sampcountry_id INTEGER NOT NULL REFERENCES ontologies_efsa.country(id),
	origcountry_id INTEGER REFERENCES ontologies_efsa.country(id),

	sampy INTEGER NOT NULL,
	sampm INTEGER,	
	sampd INTEGER,

	product_id INTEGER NOT NULL REFERENCES ontologies_efsa.product_catalogue(id),

	analysisy INTEGER NOT NULL,
	analysism INTEGER,
	analysisd INTEGER,

	repyear INTEGER,

	file_id INTEGER NOT NULL REFERENCES efsa.files(id)
	
);


-- create view with termcodes for data processing in python
-- DROP VIEW efsa.vw_sample_core_termcode;
CREATE OR REPLACE VIEW efsa.vw_sample_core_termcode
 AS
 SELECT sc.id AS sample_core_id,
    sc.analysisy,
    sampc.termcode AS sampcountry,
    l.termcode AS labaccred,
    sc.proglegalref,
    lc.termcode AS labcountry,
    sc.progid,
    sc.sampanid,
    sc.sampid,
    sc.analysism,
    sc.analysisd,
    sc.anportseq::character varying AS anportseq,
    sc.labsubsampcode,
    u.termcode AS anportsizeunit,
		sc.repyear,
		CASE
				WHEN f.id IS NOT NULL THEN f.termcode
				WHEN m.id IS NOT NULL THEN m.termcode
				ELSE NULL::character varying
		END AS sampmatcodebasebuilding,
    fi.filename AS file
   FROM efsa.sample_core sc
	 JOIN ontologies_efsa.product_catalogue pc ON sc.product_id = pc.id
     LEFT JOIN ontologies_efsa.mtx f ON pc.mtx_id = f.id
     LEFT JOIN ontologies_efsa.matrix m ON pc.matrix_id = m.id
     LEFT JOIN ontologies_efsa.labacc l ON sc.labacc_id = l.id
     LEFT JOIN ontologies_efsa.country lc ON sc.labcountry_id = lc.id
     LEFT JOIN ontologies_efsa.unit u ON sc.unit_id = u.id
     LEFT JOIN ontologies_efsa.country sampc ON sc.sampcountry_id = sampc.id
     LEFT JOIN efsa.files fi ON sc.file_id = fi.id;


-- create view for samples (core)
-- DROP VIEW efsa.vw_sample_core;
CREATE OR REPLACE VIEW efsa.vw_sample_core
 AS
 SELECT sc.id AS sample_core_id,
    sc.samplehash,
    sc.sampid,
    sc.sampanid,
    sc.progid,
    sc.proglegalref,
    p.termextendedname AS progtype,
    l.termextendedname AS labaccred,
    lc.termextendedname AS labcountry,
    sc.labsubsampcode,
    smd.termextendedname AS sampmethod,
    sstr.termextendedname AS sampstrategy,
    sc.anportseq,
    u.termextendedname AS anportsizeunit,
    snt.termextendedname AS samppoint,
    sampc.termextendedname AS sampcountry,
    origc.termextendedname AS origcountry,
    sc.sampy,
    sc.sampm,
    sc.sampd,
        CASE
            WHEN f.id IS NOT NULL THEN f.termextendedname
            WHEN m.id IS NOT NULL THEN m.termextendedname
            ELSE NULL::character varying
        END AS productname,
    sc.analysisy,
    sc.analysism,
    sc.analysisd,
		sc.repyear,
    fi.filename,
    fi.country_code,
    fi.filetype,
    pc.id AS product_id
   FROM efsa.sample_core sc
     JOIN ontologies_efsa.product_catalogue pc ON sc.product_id = pc.id
     LEFT JOIN ontologies_efsa.mtx f ON pc.mtx_id = f.id
     LEFT JOIN ontologies_efsa.matrix m ON pc.matrix_id = m.id
     LEFT JOIN ontologies_efsa.prgtyp p ON sc.prgtyp_id = p.id
     LEFT JOIN ontologies_efsa.labacc l ON sc.labacc_id = l.id
     LEFT JOIN ontologies_efsa.country lc ON sc.labcountry_id = lc.id
     LEFT JOIN ontologies_efsa.sampmd smd ON sc.sampmd_id = smd.id
     LEFT JOIN ontologies_efsa.sampstr sstr ON sc.sampstr_id = sstr.id
     LEFT JOIN ontologies_efsa.unit u ON sc.unit_id = u.id
     LEFT JOIN ontologies_efsa.sampnt snt ON sc.sampnt_id = snt.id
     LEFT JOIN ontologies_efsa.country sampc ON sc.sampcountry_id = sampc.id
     LEFT JOIN ontologies_efsa.country origc ON sc.origcountry_id = origc.id
     LEFT JOIN efsa.files fi ON sc.file_id = fi.id;


-- ----------------------------------------------------------------- SAMPLE REST
-- ------------ Just like with the sample core data, a tmp_sample_rest table is created to store the processed data from python
-- ------------ an insert query (see EFSA - INSERTS.sql) then moves data from tmp_sample_rest to sample_rest
-- ------------ sample_rest table is created below
DROP TABLE IF EXISTS efsa.tmp_sample_rest;
CREATE TABLE efsa.tmp_sample_rest (
	id SERIAL PRIMARY KEY,
	analysisd NUMERIC,
	analysism NUMERIC,
	analysisy NUMERIC,
	anportseq NUMERIC,
	anportsizeunit VARCHAR,
	file VARCHAR NOT NULL,
	labaccred VARCHAR,
	labcountry VARCHAR,
	labsubsampcode VARCHAR,
	progid VARCHAR,
	proglegalref VARCHAR,
	repyear NUMERIC,
	sampanid VARCHAR,
	sampcountry VARCHAR NOT NULL,
	sampid VARCHAR NOT NULL,
	sampmatcodebasebuilding VARCHAR,
	value VARCHAR NOT NULL,
	variable VARCHAR NOT NULL REFERENCES efsa.column_metainfo(columnname),
	samplehash uuid NOT NULL GENERATED ALWAYS AS (efsa.samplehash(	
																	sampid::text, 
																	sampanid::text,
																	progid::text,
																	proglegalref::text,
																	labaccred::text,
																	labcountry::text,
																	labsubsampcode::text,
																	anportseq::text,
																	anportsizeunit::text,
																	sampcountry::text,
																	analysisy::text,
																	analysism::text,
																	analysisd::text,
																	repyear::text,
																	sampmatcodebasebuilding::text,
																	file::text)) STORED,
	UNIQUE(samplehash, variable)
);


-- create sample_rest table
--DROP TABLE IF EXISTS efsa.sample_rest;
CREATE TABLE efsa.sample_rest (
	id SERIAL PRIMARY KEY,
	sample_core_id INTEGER NOT NULL REFERENCES efsa.sample_core(id),
	column_metainfo_id INTEGER NOT NULL REFERENCES efsa.column_metainfo(id),
	value VARCHAR NOT NULL,
	UNIQUE(sample_core_id, column_metainfo_id)
);


-- ----------------------------------------------------------------- MEASUREMENT CORE
-- ------------ Just like with the sample data, a tmp_measurement_core table is created to store the processed measurement data from python
-- ------------ an insert query (see EFSA - INSERTS.sql) then moves data from tmp_measurement_core to measurement_core
-- ------------ measurement_core table is created below
-- ------------ some views are created

DROP TABLE IF EXISTS efsa.tmp_measurement_core;
CREATE TABLE efsa.tmp_measurement_core (
	id SERIAL PRIMARY KEY,
	sample_core_id INTEGER NOT NULL,
	restype VARCHAR,
	resunit VARCHAR,
	resloq NUMERIC,
	evalcode VARCHAR,
	paramcodebaseparam VARCHAR,
	resid VARCHAR,
	resval NUMERIC,
	file_id INTEGER NOT NULL,
	measurement_nr INTEGER NOT NULL,
	UNIQUE(file_id, measurement_nr)
);


-- ------------ MEASUREMENT PARTITIONED TABLE
-- create partitioned table
--DROP TABLE IF EXISTS efsa.measurement_core;
CREATE TABLE efsa.measurement_core (
	id SERIAL,
	sample_core_id INTEGER NOT NULL REFERENCES efsa.sample_core(id),
	resid VARCHAR NOT NULL,
	filetype VARCHAR NOT NULL CHECK(filetype IN ('pesticides', 'vmpr', 'chemical')),
	param_id INTEGER NOT NULL REFERENCES ontologies_efsa.param(id),
	restyp_id INTEGER NOT NULL REFERENCES ontologies_efsa.valtyp(id),
	resunit_id INTEGER NOT NULL REFERENCES ontologies_efsa.unit(id),
	resval NUMERIC,
	resloq NUMERIC,
	evalcode_id INTEGER NOT NULL REFERENCES ontologies_efsa.reseval(id),
	file_id INTEGER NOT NULL REFERENCES efsa.files(id),
	measurement_nr INTEGER NOT NULL,
	PRIMARY KEY (id, filetype),
	UNIQUE(file_id, measurement_nr, filetype)
) PARTITION BY LIST (filetype)
;

-- create partitions
CREATE TABLE efsa.pesticides_core PARTITION OF efsa.measurement_core FOR VALUES IN ('pesticides');
CREATE TABLE efsa.vmpr_core PARTITION OF efsa.measurement_core FOR VALUES IN ('vmpr');
CREATE TABLE efsa.chemical_core PARTITION OF efsa.measurement_core FOR VALUES IN ('chemical');

-- create index
CREATE INDEX ON efsa.measurement_core(param_id);


-- create measurement core view
-- DROP VIEW efsa.vw_measurement_core;
CREATE OR REPLACE VIEW efsa.vw_measurement_core
 AS
 SELECT mc.sample_core_id,
    mc.resid,
    f.filetype,
    p.termextendedname AS param,
    p.termcode AS paramcode,
    v.termextendedname AS restype,
    v.termcode AS restypecode,
    u.termextendedname AS resunit,
    mc.resval,
    mc.resloq,
    r.termextendedname AS evalcode,
    f.id AS file_id,
    mc.measurement_nr,
    mc.param_id
   FROM efsa.measurement_core mc
     LEFT JOIN ontologies_efsa.valtyp v ON mc.restyp_id = v.id
     LEFT JOIN ontologies_efsa.unit u ON mc.resunit_id = u.id
     LEFT JOIN ontologies_efsa.reseval r ON mc.evalcode_id = r.id
     LEFT JOIN ontologies_efsa.param p ON mc.param_id = p.id
     LEFT JOIN efsa.files f ON mc.file_id = f.id;


-- create sample-measurement core view
-- DROP VIEW efsa.vw_sample_measurement_core;
CREATE OR REPLACE VIEW efsa.vw_sample_measurement_core
 AS
 SELECT sc.sample_core_id,
    sc.samplehash,
    sc.sampid,
    sc.sampanid,
    sc.progid,
    sc.proglegalref,
    sc.progtype,
    sc.labaccred,
    sc.labcountry,
    sc.labsubsampcode,
    sc.sampmethod,
    sc.sampstrategy,
    sc.anportseq,
    sc.anportsizeunit,
    sc.samppoint,
    sc.sampcountry,
    sc.origcountry,
    sc.sampy,
    sc.sampm,
    sc.sampd,
    sc.productname,
    sc.analysisy,
    sc.analysism,
    sc.analysisd,
    sc.filename,
    sc.country_code,
    sc.filetype,
    mc.resid,
    mc.param,
    mc.paramcode,
    mc.restype,
    mc.restypecode,
    mc.resunit,
    mc.resval,
    mc.resloq,
    mc.evalcode,
    mc.measurement_nr,
    mc.file_id,
    sc.product_id,
    mc.param_id
   FROM efsa.vw_sample_core sc
     JOIN efsa.vw_measurement_core mc ON sc.sample_core_id = mc.sample_core_id;


-- ----------------------------------------------------------------- MEASUREMENT REST
-- create partitioned table
--DROP TABLE IF EXISTS efsa.measurement_rest;
CREATE TABLE efsa.measurement_rest (
	id BIGSERIAL,
	sample_core_id INTEGER,
	file_id INTEGER NOT NULL REFERENCES efsa.files(id),
	measurement_nr INTEGER NOT NULL,
	columnname VARCHAR NOT NULL REFERENCES efsa.column_metainfo(columnname),
	value VARCHAR NOT NULL,
	PRIMARY KEY(id, columnname),
	UNIQUE(file_id, measurement_nr, columnname)
) PARTITION BY LIST (columnname)
;

-- create partitions
CREATE TABLE efsa.paramtype PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramtype');
CREATE TABLE efsa.accredproc PARTITION OF efsa.measurement_rest FOR VALUES IN ('accredproc');
CREATE TABLE efsa.anmethrefid PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethrefid');
CREATE TABLE efsa.anmethtype PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethtype');
CREATE TABLE efsa.anmethcodebasemeth PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethcodebasemeth');
CREATE TABLE efsa.exprrestype PARTITION OF efsa.measurement_rest FOR VALUES IN ('exprrestype');
CREATE TABLE efsa.paramcode PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcode');
CREATE TABLE efsa.resevaluation PARTITION OF efsa.measurement_rest FOR VALUES IN ('resevaluation');
CREATE TABLE efsa.resultcode PARTITION OF efsa.measurement_rest FOR VALUES IN ('resultcode');
CREATE TABLE efsa.anmethtext PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethtext');
CREATE TABLE efsa.resvalreccorr PARTITION OF efsa.measurement_rest FOR VALUES IN ('resvalreccorr');
CREATE TABLE efsa.reslod PARTITION OF efsa.measurement_rest FOR VALUES IN ('reslod');
CREATE TABLE efsa.evallimittype PARTITION OF efsa.measurement_rest FOR VALUES IN ('evallimittype');
CREATE TABLE efsa.resqualvalue PARTITION OF efsa.measurement_rest FOR VALUES IN ('resqualvalue');
CREATE TABLE efsa.anmethcode PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethcode');
CREATE TABLE efsa.acttakencode PARTITION OF efsa.measurement_rest FOR VALUES IN ('acttakencode');
CREATE TABLE efsa.anmethrefcode PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethrefcode');
CREATE TABLE efsa.reslegallimittype PARTITION OF efsa.measurement_rest FOR VALUES IN ('reslegallimittype');
CREATE TABLE efsa.exprres PARTITION OF efsa.measurement_rest FOR VALUES IN ('exprres');
CREATE TABLE efsa.ccbeta PARTITION OF efsa.measurement_rest FOR VALUES IN ('ccbeta');
CREATE TABLE efsa.reslc PARTITION OF efsa.measurement_rest FOR VALUES IN ('reslc');
CREATE TABLE efsa.resvalllb PARTITION OF efsa.measurement_rest FOR VALUES IN ('resvalllb');
CREATE TABLE efsa.resvallmb PARTITION OF efsa.measurement_rest FOR VALUES IN ('resvallmb');
CREATE TABLE efsa.resvallub PARTITION OF efsa.measurement_rest FOR VALUES IN ('resvallub');
CREATE TABLE efsa.resvalrec PARTITION OF efsa.measurement_rest FOR VALUES IN ('resvalrec');
CREATE TABLE efsa.ccalpha PARTITION OF efsa.measurement_rest FOR VALUES IN ('ccalpha');
CREATE TABLE efsa.evallowlimit PARTITION OF efsa.measurement_rest FOR VALUES IN ('evallowlimit');
CREATE TABLE efsa.evalinfosamptkasses PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinfosamptkasses');
CREATE TABLE efsa.resvaluncert PARTITION OF efsa.measurement_rest FOR VALUES IN ('resvaluncert');
CREATE TABLE efsa.reslegallimit PARTITION OF efsa.measurement_rest FOR VALUES IN ('reslegallimit');
CREATE TABLE efsa.resrefid PARTITION OF efsa.measurement_rest FOR VALUES IN ('resrefid');
CREATE TABLE efsa.exprrespercfatperc PARTITION OF efsa.measurement_rest FOR VALUES IN ('exprrespercfatperc');
CREATE TABLE efsa.evalinforesultassess PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinforesultassess');
CREATE TABLE efsa.exprrespercmoistperc PARTITION OF efsa.measurement_rest FOR VALUES IN ('exprrespercmoistperc');
CREATE TABLE efsa.evalinfosampeventasses PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinfosampeventasses');
CREATE TABLE efsa.resinfonotsummed PARTITION OF efsa.measurement_rest FOR VALUES IN ('resinfonotsummed');
CREATE TABLE efsa.anmethinfocom PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfocom');
CREATE TABLE efsa.resulwr PARTITION OF efsa.measurement_rest FOR VALUES IN ('resulwr');
CREATE TABLE efsa.evalinfoconclusion PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinfoconclusion');
CREATE TABLE efsa.evalinfosampanasses PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinfosampanasses');
CREATE TABLE efsa.resvaluncertsd PARTITION OF efsa.measurement_rest FOR VALUES IN ('resvaluncertsd');
CREATE TABLE efsa.exprrespercalcoholperc PARTITION OF efsa.measurement_rest FOR VALUES IN ('exprrespercalcoholperc');
CREATE TABLE efsa.evalhighlimit PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalhighlimit');
CREATE TABLE efsa.anmethinfo PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfo');
CREATE TABLE efsa.anmethcodesampintro PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethcodesampintro');
CREATE TABLE efsa.anmethinfocontacttempc PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfocontacttempc');
CREATE TABLE efsa.anmethinfocontacttimemin PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfocontacttimemin');
CREATE TABLE efsa.anmethinfodiskconc PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfodiskconc');
CREATE TABLE efsa.anmethinfodiskdiam PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfodiskdiam');
CREATE TABLE efsa.anmethinfomethsensitivity PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfomethsensitivity');
CREATE TABLE efsa.anmethinfomethspecificity PARTITION OF efsa.measurement_rest FOR VALUES IN ('anmethinfomethspecificity');
CREATE TABLE efsa.efsaparamcode PARTITION OF efsa.measurement_rest FOR VALUES IN ('efsaparamcode');
CREATE TABLE efsa.evalinfosyntestasses PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinfosyntestasses');
CREATE TABLE efsa.evalinfotseindexcase PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinfotseindexcase');
CREATE TABLE efsa.evalinfotsenationalcaseid PARTITION OF efsa.measurement_rest FOR VALUES IN ('evalinfotsenationalcaseid');
CREATE TABLE efsa.paramcodeag PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodeag');
CREATE TABLE efsa.paramcodeallele PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodeallele');
CREATE TABLE efsa.paramcodeampc PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodeampc');
CREATE TABLE efsa.paramcodeanth PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodeanth');
CREATE TABLE efsa.paramcodecarbapenem PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodecarbapenem');
CREATE TABLE efsa.paramcodecc PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodecc');
CREATE TABLE efsa.paramcodeesbl PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodeesbl');
CREATE TABLE efsa.paramcodeexptype PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodeexptype');
CREATE TABLE efsa.paramcodest PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodest');
CREATE TABLE efsa.paramcodet PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodet');
CREATE TABLE efsa.paramcodevt PARTITION OF efsa.measurement_rest FOR VALUES IN ('paramcodevt');
CREATE TABLE efsa.resinfomlvaprofile PARTITION OF efsa.measurement_rest FOR VALUES IN ('resinfomlvaprofile');
CREATE TABLE efsa.resinfopercc PARTITION OF efsa.measurement_rest FOR VALUES IN ('resinfopercc');
CREATE TABLE efsa.resinfopermlst PARTITION OF efsa.measurement_rest FOR VALUES IN ('resinfopermlst');
CREATE TABLE efsa.resinforeftypeprov PARTITION OF efsa.measurement_rest FOR VALUES IN ('resinforeftypeprov');
CREATE TABLE efsa.resllwr PARTITION OF efsa.measurement_rest FOR VALUES IN ('resllwr');
CREATE TABLE efsa.resrefidbaseattachmentid PARTITION OF efsa.measurement_rest FOR VALUES IN ('resrefidbaseattachmentid');
CREATE TABLE efsa.resrefidlaneref PARTITION OF efsa.measurement_rest FOR VALUES IN ('resrefidlaneref');



DROP VIEW efsa.vw_sample_measurement_core_termcode;

CREATE OR REPLACE VIEW efsa.vw_sample_measurement_core_termcode
 AS
 SELECT sc.id AS sample_core_id,
    sc.analysisy,
    sampc.termcode AS sampcountry,
    l.termcode AS labaccred,
    lc.termcode AS labcountry,
    origc.termcode AS origcountry,
    smd.termcode AS sampmethod,
    sstr.termcode AS sampstrategy,
    snt.termcode AS samppoint,
    pr.termcode AS progtype,
    sc.progid,
    sc.proglegalref,
    sc.sampanid,
    sc.sampid,
    sc.analysism,
    sc.analysisd,
    sc.sampy,
    sc.sampm,
    sc.sampd,
    sc.repyear,
    sc.anportseq::character varying AS anportseq,
    sc.labsubsampcode,
        CASE
            WHEN f.id IS NOT NULL THEN f.termcode
            WHEN m.id IS NOT NULL THEN m.termcode
            ELSE NULL::character varying
        END AS sampmatcodebasebuilding,
    u.termcode AS anportsizeunit,
    p.termcode AS paramcodebaseparam,
    v.termcode AS restype,
    u2.termcode AS resunit,
    r.termcode AS evalcode,
    mc.resid,
    mc.resloq,
    mc.resval,
    mc.file_id,
    mc.measurement_nr,
    fi.filename
   FROM efsa.sample_core sc
     JOIN efsa.measurement_core mc ON sc.id = mc.sample_core_id
     JOIN ontologies_efsa.product_catalogue pc ON sc.product_id = pc.id
     LEFT JOIN ontologies_efsa.mtx f ON pc.mtx_id = f.id
     LEFT JOIN ontologies_efsa.matrix m ON pc.matrix_id = m.id
     LEFT JOIN ontologies_efsa.labacc l ON sc.labacc_id = l.id
     LEFT JOIN ontologies_efsa.country lc ON sc.labcountry_id = lc.id
     LEFT JOIN ontologies_efsa.unit u ON sc.unit_id = u.id
     LEFT JOIN ontologies_efsa.country sampc ON sc.sampcountry_id = sampc.id
     LEFT JOIN efsa.files fi ON sc.file_id = fi.id
     LEFT JOIN ontologies_efsa.param p ON mc.param_id = p.id
     LEFT JOIN ontologies_efsa.valtyp v ON mc.restyp_id = v.id
     LEFT JOIN ontologies_efsa.unit u2 ON mc.resunit_id = u2.id
     LEFT JOIN ontologies_efsa.reseval r ON mc.evalcode_id = r.id
     LEFT JOIN ontologies_efsa.country origc ON sc.origcountry_id = origc.id
     LEFT JOIN ontologies_efsa.prgtyp pr ON sc.prgtyp_id = pr.id
     LEFT JOIN ontologies_efsa.sampmd smd ON sc.sampmd_id = smd.id
     LEFT JOIN ontologies_efsa.sampstr sstr ON sc.sampstr_id = sstr.id
     LEFT JOIN ontologies_efsa.sampnt snt ON sc.sampnt_id = snt.id;