-- extra catalogue table to deal with mtx and matrix codes in "sampmatcodebasebuilding" in efsa.sample_core
CREATE TABLE ontologies_efsa.product_catalogue (
	id SERIAL PRIMARY KEY,
	mtx_id INTEGER REFERENCES ontologies_efsa.mtx(id),
	matrix_id INTEGER REFERENCES ontologies_efsa.matrix(id),
	UNIQUE(mtx_id, matrix_id)
);

INSERT INTO ontologies_efsa.product_catalogue(mtx_id) SELECT id FROM ontologies_efsa.mtx;
INSERT INTO ontologies_efsa.product_catalogue(matrix_id) SELECT id FROM ontologies_efsa.matrix;