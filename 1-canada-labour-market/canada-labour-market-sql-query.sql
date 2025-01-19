CREATE TABLE pub (
	rec_num bigint,
	survyear smallint,
	survmnth smallint,
	lfsstat varchar(2),
	prov varchar(2),
	cma varchar(1),
	age_12 varchar(2),
	age_6 varchar(1),
	sex varchar(1),
	marstat varchar(1),
	educ varchar(1),
	mjh varchar(1),
	everwork varchar(1),
	ftptlast varchar(1),
	cowmain varchar(1),
	immig varchar(1),
	naics_21 varchar(2),
	noc_10 varchar(2),
	noc_43 varchar(2),
	yabsent varchar(1),
	wksaway smallint,
	payaway varchar(1),
	uhrsmain smallint,
	ahrsmian smallint,
	ftptmain varchar(1),
	utothrs smallint,
	atothur smallint,
	hrsaway smallint,
	yaway varchar(1),
	paidot smallint,
	unpaidot smallint,
	xtrahrs smallint,
	whypt varchar(1),
	tenure smallint,
	prevten smallint,
	hrlyearn integer,
	"union" varchar(1),
	permtemp varchar(1),
	estsize varchar(1),
	firmsize varchar(1),
	durunemp varchar(2),
	flowunem varchar(1),
	unemftpt varchar(1),
	whylefto varchar(1),
	whyleftn varchar(2),
	durjless smallint,
	available varchar(1),
	lkpubag varchar(1),
	lkemploy varchar(1),
	lkrels varchar(1),
	lkatads varchar(1),
	lkansads varchar(1),
	lkothern varchar(1),
	prioract varchar(1),
	ynolook varchar(1),
	tlolook varchar(1),
	schooln varchar(1),
	efamtype varchar(2),
	agyownk varchar(1),
	finalwt integer
);


COPY pub
FROM 'D:\01 Project\Monthly Canadian job market analysis\datasets\pub0120.csv'
WITH (FORMAT CSV, HEADER);

SELECT COUNT(rec_num) FROM pub;



--Read monthly files and check the number of rows
CREATE OR REPLACE PROCEDURE read(mmyy varchar(4))
as $$
BEGIN
	EXECUTE format(
		'COPY pub FROM %L WITH (FORMAT CSV, HEADER)',
		'D:\01 Project\Monthly Canadian job market analysis\datasets\pub'||mmyy||'.csv'
	);
	RAISE NOTICE 'Copy completed for %', mmyy;
END;
$$
LANGUAGE plpgsql;

CREATE TABLE count_chk(
	year_cnt integer,
	month_cnt integer,
	cnt integer
);

CREATE OR REPLACE PROCEDURE cnt_chk(
	yyyy integer,
	mm integer
)
AS $$
BEGIN
	INSERT INTO count_chk (year_cnt, month_cnt, cnt)
	SELECT yyyy, mm, COUNT(rec_num)
	FROM pub
	WHERE survyear = yyyy AND survmnth = mm
	GROUP BY survyear, survmnth;
END;
$$
LANGUAGE plpgsql;

CALL read('0124');
CALL cnt_chk(2024, 1); SELECT * FROM count_chk;
CALL read('0224');
CALL cnt_chk(2024, 2); --SELECT * FROM count_chk;
CALL read('0324');
CALL cnt_chk(2024, 3); --SELECT * FROM count_chk;
CALL read('0424');
CALL cnt_chk(2024, 4); --SELECT * FROM count_chk;
CALL read('0524');
CALL cnt_chk(2024, 5); --SELECT * FROM count_chk;
CALL read('0624');
CALL cnt_chk(2024, 6); --SELECT * FROM count_chk;
CALL read('0724');
CALL cnt_chk(2024, 7); --SELECT * FROM count_chk;
CALL read('0824');
CALL cnt_chk(2024, 8); --SELECT * FROM count_chk;
CALL read('0924');
CALL cnt_chk(2024, 9); SELECT * FROM count_chk;
/*CALL read('1023');
CALL cnt_chk(2024, 10); --SELECT * FROM count_chk;
CALL read('1123');
CALL cnt_chk(2024, 11); --SELECT * FROM count_chk;
CALL read('1223');
CALL cnt_chk(2024, 12); SELECT * FROM count_chk;*/

SELECT COUNT(rec_num) FROM pub;
SELECT * FROM count_chk;




CREATE TABLE pub2_columns AS
	SELECT id, survyear, survmnth, lfsstat, permtemp, ftptmain, 
			prov, cma, naics_21, noc_10, noc_43, "union" as unions, firmsize, cowmain,
			ahrsmian as ahrsmain,
			paidot, unpaidot, tenure, hrlyearn
	FROM pub;

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'pub2_columns';

SELECT * FROM pub2_columns
LIMIT 5;



-- Keep only rows meeting all conditions:
-- public/private sector employees, full-time/permanent emplyees, and CMAs
DELETE FROM pub2_columns
	WHERE (lfsstat != '1') OR
		(cowmain NOT IN ('1', '2')) OR
		(ftptmain != '1') OR
		(permtemp != '1') OR
		(cma = '0');

SELECT COUNT(id) FROM pub2_columns;
SELECT survyear, survmnth, COUNT(id) FROM pub2_columns
GROUP BY survyear, survmnth;



--Idenfity outliers
CREATE TABLE tenure_outlier AS
WITH stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY tenure) AS Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tenure) AS Q3
    FROM pub2_columns
)
SELECT *
FROM pub2_columns, stats
WHERE tenure < (Q1 - 1.5 * (Q3 - Q1))
   OR tenure > (Q3 + 1.5 * (Q3 - Q1));
SELECT COUNT(id) FROM tenure_outlier;

CREATE TABLE hrlyearn_outlier AS
WITH stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY hrlyearn) AS Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY hrlyearn) AS Q3
    FROM pub2_columns
)
SELECT *
FROM pub2_columns, stats
WHERE hrlyearn < (Q1 - 1.5 * (Q3 - Q1))
   OR hrlyearn > (Q3 + 1.5 * (Q3 - Q1));
SELECT COUNT(id) FROM hrlyearn_outlier;

SELECT * FROM hrlyearn_outlier LIMIT 5;
SELECT survyear, survmnth, COUNT(id) FROM hrlyearn_outlier
GROUP BY survyear, survmnth
ORDER BY survyear, survmnth;


--Convert 'CMA' codes into their respective CMA names
UPDATE pub2_columns
SET cma_text =
	CASE
		WHEN cma = '1' THEN 'Quebec City'
		WHEN cma = '2' THEN 'Montreal'
		WHEN cma = '3' THEN 'Ottawa'
		WHEN cma = '4' THEN 'Toronto'
		WHEN cma = '5' THEN 'Hamilton'
		WHEN cma = '6' THEN 'Winnipeg'
		WHEN cma = '7' THEN 'Calgary'
		WHEN cma = '8' THEN 'Edmonton'
		WHEN cma = '9' THEN 'Vancouver'
		ELSE 'Error'
	END;
SELECT cma_text, count(cma_text) FROM pub2_columns
GROUP BY cma_text
ORDER BY count(cma_text) DESC;




--Combine year and month fields into a single field and convert to date type
ALTER TABLE pub2_date
ADD COLUMN surv_date DATE;

UPDATE pub2_date
SET surv_date = TO_DATE(survyear || '-' || LPAD(survmnth::TEXT, 2, '0') || '-01', 'YYYY-MM-DD');

SELECT
	COUNT (CASE WHEN survyear=2021 AND survmnth=1 THEN 1 END) AS year_mnth_cnt,
	COUNT (CASE WHEN surv_date='2021-01-01' THEN 1 END) AS date_cnt
FROM pub2_date;




CREATE TABLE canada_labour_market AS
SELECT id, surv_date, cma_text, naics_21, noc_10, unions, tenure, hrlyearn
FROM pub2_date;
SELECT COUNT(DISTINCT id) FROM canada_labour_market;




--Wage growth calculation
--not used in final results
/*DROP TABLE IF EXISTS pub2_naics_wage_growth_table;
CREATE TABLE pub2_naics_wage_growth_table AS
SELECT surv_date, naics_21, naics_wage_median, pre_naics_wage_median,
	CASE 
		WHEN pre_naics_wage_median IS NOT NULL THEN
		 ROUND( (( naics_wage_median / pre_naics_wage_median)-1)::numeric, 4)
		ELSE NULL
	END AS naics_growth
FROM (
	SELECT surv_date, naics_21,
		PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hrlyearn) AS naics_wage_median,
		LAG(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hrlyearn)) OVER (
			PARTITION BY naics_21
			ORDER BY surv_date
		) AS pre_naics_wage_median
	FROM canada_labour_market
	GROUP BY naics_21, surv_date
);
SELECT * FROM pub2_naics_wage_growth_table;

DROP TABLE IF EXISTS pub2_cma_wage_growth_table;
CREATE TABLE pub2_cma_wage_growth_table AS
SELECT surv_date, cma_text, cma_wage_median, pre_cma_wage_median,
	CASE 
		WHEN pre_cma_wage_median IS NOT NULL THEN
		 ROUND( (( cma_wage_median / pre_cma_wage_median)-1)::numeric, 4)
		ELSE NULL
	END AS cma_growth
FROM (
	SELECT surv_date, cma_text,
		PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hrlyearn) AS cma_wage_median,
		LAG(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hrlyearn)) OVER (
			PARTITION BY cma_text
			ORDER BY surv_date
		) AS pre_cma_wage_median
	FROM canada_labour_market
	GROUP BY cma_text, surv_date
);
SELECT * FROM pub2_cma_wage_growth_table;
*/



ALTER TABLE canada_labour_market
ADD CONSTRAINT canada_labour_market_pk PRIMARY KEY (id);

COPY canada_labour_market
TO 'D:\01 Project\Monthly Canadian job market analysis\canada_labour_market.csv'
WITH (FORMAT CSV, HEADER);