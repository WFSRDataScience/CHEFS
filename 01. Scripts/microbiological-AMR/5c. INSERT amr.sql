-- insert values from tmp_amr into efsa.amr
INSERT INTO efsa.amr SELECT * FROM tmp_amr;