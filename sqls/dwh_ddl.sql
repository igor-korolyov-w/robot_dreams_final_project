--dim tables

CREATE TABLE public.dim_aisles (
	aisle_id int4 NOT NULL,
	aisle varchar(51) NOT NULL
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (aisle_id);

CREATE TABLE public.dim_clients (
	client_id serial4 NOT NULL,
	fullname varchar(127) NOT NULL,
	location_area_id int2 NOT NULL
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (client_id);


CREATE TABLE public.dim_departments (
	department_id int4 NOT NULL,
	department_name varchar(51) NOT NULL
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (department_id);


CREATE TABLE public.dim_products (
	product_id int4 NOT NULL,
	product_name varchar(127) NOT NULL,
	aisle_id int4 NOT NULL,
	department_id int4 NOT NULL
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (product_id);


CREATE TABLE public.dim_time (
	action_date date NOT NULL,
	DayWeek int4 NOT NULL,
	DayMonth int4 NOT NULL,
	DayQuarter int4 NOT NULL,
	DayYear int4 NOT NULL,
	DayName varchar(16) NOT NULL,
	WeekMonth int4 NOT NULL,
	WeekQuarter int4 NOT NULL,
	WeekYear int4 NOT NULL,	
	MonthQuarter int4 NOT NULL,
	MonthYear int4 NOT NULL,
	MonthName varchar(16) NOT NULL,
	QuarterYear int4 NOT NULL,
	QuarterName varchar(16) NOT NULL,
	Year int4 NOT NULL
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (action_date);

--fact tables

CREATE TABLE public.fact_orders (
	order_id int8 NOT NULL,
	client_id int4 NOT NULL,
	product_id int4 NOT NULL,
	quantity int4 NOT NULL,
	order_date date NOT null
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (client_id);

CREATE TABLE public.fact_oos (
	product_id int4 NOT NULL,
	oos_date date NOT null
)
with (appendoptimized=true, orientation=column, compresstype=zlib, compresslevel=5)
distributed by (oos_date);


