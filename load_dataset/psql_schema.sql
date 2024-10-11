CREATE SCHEMA gold;

CREATE TABLE gold.sales_values_by_category (
	monthly text NULL,
	category text NULL,
	sales float8 NULL,
	bills int8 NULL,
	values_per_bill float8 NULL
);
