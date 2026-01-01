

-- cleaning the data 

select * from online_retail limit 100;
select count(*) from Online_Retail t ;

-- total rows 541909
-- 135,080 rows have no customer_id

create table online_retail_main as 
with online_retail as (
	select 
		*
	from online_retail 
	where customerid is not null
),
quantity_unit_price as (
	select *
	from online_retail 
	where quantity > 0 and unitprice > 0 
),
dup_chk as ( 
	-- duplicate check 
	select *,
		row_number() over (partition by invoiceno, stockcode, quantity order by invoicedate ) dup_flag
	from quantity_unit_price 
) -- 392,669 of clean data  and 5,215 of duplicated rows removed 
select * 
from dup_chk 
where dup_flag = 1;


-- clean data 

-- begin cohort analysis 

select * from online_retail_main ;


ALTER TABLE online_retail_main
ADD COLUMN invoicedate_dt DATETIME;

UPDATE online_retail_main
SET invoicedate_dt = STR_TO_DATE(invoicedate, '%m/%d/%y %H:%i');





-- unique identifier is the customer_id
-- initial start date (first invoice date)
-- revenue data 



create  table cohort as 
select 
	customerid,
	min(invoicedate_dt) as first_purchase_date,
	date_format( min(invoicedate_dt ), '%Y-%m-01' ) as cohort_date
from online_retail_main
group by customerid; 


select * from cohort


-- cohort index is an integer representation of the number of months that has passed since the customers first engagement 

-- creating cohort index

create table cohort_retention as 
select
	mmm.*,
	year_diff * 12 + month_diff + 1 as cohort_index
from(
		select
			mm.*,
			invoice_year - cohort_year as year_diff ,
			invoice_month - cohort_month as month_diff
		from(
				select 
					m.*,
					c.cohort_date,
					year(m.invoicedate_dt) as invoice_year,
					month(m.invoicedate_dt) as invoice_month,
					year(c.cohort_date)	as cohort_year,
					month(c.cohort_date)	as cohort_month
				from online_retail_main m 
				left join cohort  c on m.customerid = c.customerid
		)mm
)mmm

-- drop table cohort_retention;


-- create view cohort_final as 
select * from cohort_retention;


-- creating the pivot table ( in counts )

SELECT
    cohort_date,
    COUNT(DISTINCT CASE WHEN cohort_index = 1  THEN customerid END) AS m1,
    COUNT(DISTINCT CASE WHEN cohort_index = 2  THEN customerid END) AS m2,
    COUNT(DISTINCT CASE WHEN cohort_index = 3  THEN customerid END) AS m3,
    COUNT(DISTINCT CASE WHEN cohort_index = 4  THEN customerid END) AS m4,
    COUNT(DISTINCT CASE WHEN cohort_index = 5  THEN customerid END) AS m5,
    COUNT(DISTINCT CASE WHEN cohort_index = 6  THEN customerid END) AS m6,
    COUNT(DISTINCT CASE WHEN cohort_index = 7  THEN customerid END) AS m7,
    COUNT(DISTINCT CASE WHEN cohort_index = 8  THEN customerid END) AS m8,
    COUNT(DISTINCT CASE WHEN cohort_index = 9  THEN customerid END) AS m9,
    COUNT(DISTINCT CASE WHEN cohort_index = 10 THEN customerid END) AS m10,
    COUNT(DISTINCT CASE WHEN cohort_index = 11 THEN customerid END) AS m11,
    COUNT(DISTINCT CASE WHEN cohort_index = 12 THEN customerid END) AS m12,
    COUNT(DISTINCT CASE WHEN cohort_index = 13 THEN customerid END) AS m13
FROM cohort_retention
GROUP BY cohort_date
ORDER BY cohort_date;




-- pivot table (in percentage)

WITH cohort_counts AS (
    SELECT
        cohort_date,
        COUNT(DISTINCT CASE WHEN cohort_index = 1  THEN customerid END) AS m1,
        COUNT(DISTINCT CASE WHEN cohort_index = 2  THEN customerid END) AS m2,
        COUNT(DISTINCT CASE WHEN cohort_index = 3  THEN customerid END) AS m3,
        COUNT(DISTINCT CASE WHEN cohort_index = 4  THEN customerid END) AS m4,
        COUNT(DISTINCT CASE WHEN cohort_index = 5  THEN customerid END) AS m5,
        COUNT(DISTINCT CASE WHEN cohort_index = 6  THEN customerid END) AS m6,
        COUNT(DISTINCT CASE WHEN cohort_index = 7  THEN customerid END) AS m7,
        COUNT(DISTINCT CASE WHEN cohort_index = 8  THEN customerid END) AS m8,
        COUNT(DISTINCT CASE WHEN cohort_index = 9  THEN customerid END) AS m9,
        COUNT(DISTINCT CASE WHEN cohort_index = 10 THEN customerid END) AS m10,
        COUNT(DISTINCT CASE WHEN cohort_index = 11 THEN customerid END) AS m11,
        COUNT(DISTINCT CASE WHEN cohort_index = 12 THEN customerid END) AS m12,
        COUNT(DISTINCT CASE WHEN cohort_index = 13 THEN customerid END) AS m13
    FROM cohort_retention
    GROUP BY cohort_date
)

SELECT
    cohort_date,
    100.0 * m1 / m1  AS m1_pct,
    100.0 * m2 / m1  AS m2_pct,
    100.0 * m3 / m1  AS m3_pct,
    100.0 * m4 / m1  AS m4_pct,
    100.0 * m5 / m1  AS m5_pct,
    100.0 * m6 / m1  AS m6_pct,
    100.0 * m7 / m1  AS m7_pct,
    100.0 * m8 / m1  AS m8_pct,
    100.0 * m9 / m1  AS m9_pct,
    100.0 * m10 / m1 AS m10_pct,
    100.0 * m11 / m1 AS m11_pct,
    100.0 * m12 / m1 AS m12_pct,
    100.0 * m13 / m1 AS m13_pct
FROM cohort_counts
ORDER BY cohort_date;


