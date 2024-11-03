-- загрузка таблиц 

-- gpfdist

drop table if exists std7_59.stores;
create table std7_59.stores(
plant bpchar(4) null,
txt varchar(50) null
)
distributed replicated;

drop table if exists std7_59.coupons;
create table std7_59.coupons(
plant bpchar(4) null,
c_num varchar(10) null,
id_promo varchar(36) null,
material int8 null,
billnum int8 null,
price_in_bill int8 null,
promo_type bpchar(3) null,
amount_price numeric(5, 2) null,
"date" date null
)
distributed replicated;

drop table if exists std7_59.promos;
create table std7_59.promos(
id_promo varchar(36) null,
promo_name varchar(30) null,
promo_type bpchar(3) null,
material int8 NULL,
promo_amount int4 null
)
distributed replicated;

drop table if exists std7_59.promo_types;
create table std7_59.promo_types(
promo_type bpchar(3) null,
txt varchar(100) null
)
distributed replicated;

-- PXF

drop table if exists std7_59.traffic;
create table std7_59.traffic(
plant bpchar(4) NULL,
"date" date NULL,
"time" bpchar(6) NULL,
frame_id bpchar(10) NULL,
quantity int4 NULL
)
with (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1	
)
distributed randomly           -- нет подходящих столбцов под ключ
partition by range ("date")
(
	start ('2021-01-01'::date) 
	end ('2021-03-01'::date) 
	every ('1 month'::interval)
	--default partition def_pa -- дефолтная партици туда попадают данные не подходящие для выборки 
);


drop table if exists std7_59.bills_head;
create table std7_59.bills_head(
billnum int8 NULL,
plant bpchar(4) NULL,
calday date NULL
)
with (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1	
)
distributed randomly           -- нет подходящих столбцов под ключ
partition by range (calday)
(
	start ('2021-01-01'::date) 
	end ('2021-03-01'::date) 
	every ('1 month'::interval)
	--default partition def_pa -- дефолтная партици туда попадают данные не подходящие для выборки 
);

drop table if exists std7_59.bills_item;
create table std7_59.bills_item(
billnum int8 NULL,
billitem int8 NULL,
material int8 NULL,
qty int8 NULL,
netval numeric(17, 2) NULL,
tax numeric(17, 2) NULL,
rpa_sat numeric(17, 2) NULL,
calday date NULL
)
with (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1	
)
distributed randomly           -- нет подходящих столбцов под ключ
partition by range (calday)
(
	start ('2021-01-01'::date) 
	end ('2021-03-01'::date) 
	every ('1 month'::interval)
	--default partition def_pa -- дефолтная партици туда попадают данные не подходящие для выборки 
);

-- создание внешних таблиц PXF

drop external table if exists std7_59.traffic_ext;
create external table std7_59.traffic_ext(
	plant bpchar(4),
	"date" bpchar(10),
	"time" bpchar(6),
	frame_id bpchar(10),
	quantity int4 
)
location ('pxf://gp.traffic?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.000.000:5432/postgres&USER=&PASS='
) on all
format 'CUSTOM' (FORMATTER='pxfwritable_import')
encoding 'UTF8';

select plant, to_date("date", 'DD MM YYYY') date from std7_59.traffic_ext;

insert into std7_59.traffic(plant, "date", "time", frame_id, quantity)
select plant, to_date("date", 'DD MM YYYY'), "time", frame_id, quantity from std7_59.traffic_ext;

select * from std7_59.traffic;

select gp_segment_id, count(1) from std7_59.traffic group by 1;  -- проверка распределения
select (gp_toolkit.gp_skew_coefficient('std7_59.traffic'::regclass)).skccoeff;  -- коэфицент перекоса
select * from std7_59.traffic_1_prt_2;  -- показ партиций

-- создание внешних таблиц gpfdist

drop external table if exists std7_59.coupons_ext;
create external table std7_59.coupons_ext(
plant varchar,
c_num varchar,
id_promo varchar(36),
material varchar,
billnum int,
price_in_bill int,
promo_type varchar,
amount_price numeric(5, 2),
"date" date
)
location ('gpfdist://172.16.000.00:8081/coupons5.csv'
) on all 
format 'CSV' (delimiter ','  null '' escape '"' quote '"')
encoding 'UTF8'
segment reject limit 10 rows;

-- gpfdist -p 8081 

insert into std7_59.coupons(plant, "date", c_num, id_promo, material, billnum, price_in_bill, promo_type, amount_price)
select plant, "date", c_num, id_promo, material, billnum, price_in_bill, promo_type, amount_price from std7_59.coupons_ext;


select * from std7_59.coupons;

-- функция загрузки справочников и небольших таблиц full

create or replace function std7_59.load_full_ch(p_table text, p_file_name text) returns int4 -- таблица для загрузки и исходный файл с данными
language plpgsql
volatile
as $$

declare
v_ext_table_name text; -- название внешней таблицы
v_sql text;            -- sql скрипт
v_gpfdist text;		   -- протокол подключения 	
v_result int;		   -- возвращаемое значение	

begin
	v_ext_table_name = p_table||'_ext';  -- имя внешней таблицы
	execute 'truncate table '||p_table;   -- очистка таблицы
	execute 'drop external table if exists '||v_ext_table_name; -- удаление временной таблицы если существует
	v_gpfdist = 'gpfdist://172.16.128.000:8081/'||p_file_name||'.csv'; -- подключение gpfdist
	
    -- создание внешней таблицы
	v_sql = 'create external table '||v_ext_table_name||'(like '||p_table||')  
		location ('''||v_gpfdist||''') on all
		format ''csv'' (header delimiter '','' null '''' escape ''"'' quote ''"'')
		encoding ''UTF8''';
	raise notice 'external table is %', v_sql;  -- вывод сгенерированный скрипт
	execute v_sql;
	execute 'insert into '||p_table||' select * from '||v_ext_table_name; --вставка данных из временной таблицы
	execute 'select count(1) from '||p_table into v_result; -- подсчет вставленных строк

	perform std7_59.f_analyze_table(p_table_name := p_table);			

	return v_result;
end;
$$
execute on any;


select std7_59.load_full_ch('std7_59.promos', 'promos2');

select std7_59.load_full_ch('std7_59.stores', 'stores2');

select * from std7_59.coupons;


--
CREATE OR REPLACE FUNCTION std7_59.f_unify_name(p_name text)				
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
BEGIN  
	/* Заменяет символы в string, найденные в наборе from, на соответствующие символы в множестве to.  */
	RETURN lower(trim(translate(p_name, ';/''','')));
END;

$$
EXECUTE ON ANY;


--
CREATE OR REPLACE FUNCTION std7_59.f_get_table_schema(p_table text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
  v_table    text;
  v_schema   text;
BEGIN
   v_table = std7_59.f_unify_name(p_name := p_table);  
   v_schema = case 
	when position('.' in v_table) = 0					--Положение указанной подстроки
     then ''
    else
     left(v_table, position('.' in v_table)-1)
   end;
   return v_schema;
END;

$$
EXECUTE ON ANY;

--
CREATE OR REPLACE FUNCTION std7_59.f_get_table_name(p_table text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
DECLARE
  v_table    text;
BEGIN
   v_table = std7_59.f_unify_name(p_name := p_table);
   v_table = case 
	when position('.' in v_table) = 0
     then ''
    else
     right(v_table, length(v_table) - position('.' in v_table))  
   end;
   return v_table;
END;

$$
EXECUTE ON ANY;

--
create or replace function std7_59.f_analyze_table(p_table_name text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
  v_table_name     text;
  v_sql            text;
BEGIN
  v_table_name := std7_59.f_unify_name(p_name := p_table_name);
 
  v_sql := 'ANALYZE '||v_table_name;
  EXECUTE v_sql;

END

$$
EXECUTE ON ANY;


--
create or replace function std7_59.f_load_delta_partition(p_table text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, p_user_id text, p_pass text)

	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare
	v_ext_table			text;
	v_temp_table		text;
	v_schema_name		text;
	v_pxf_table			text;
	v_sql				text;
	v_pxf				text;
	v_result			int;
	v_dist_key			text;
	v_params			text;
	v_where				text;
	v_load_interval		interval;
	v_start_date		date;
	v_end_date			date;
	v_table_oid			int4;
	v_cnt				int8;
	v_check_partition	int8;
	v_temp_table_2		text;
	v_main_table		text;

begin
	--создаю переменные с помощью функций
	v_ext_table = std7_59.f_unify_name(p_name := p_table)||'_ext';
	v_temp_table = std7_59.f_unify_name(p_name := p_table)||'_temp';
	v_temp_table_2 = std7_59.f_unify_name(p_name := p_table)||'_temp2';
	v_schema_name = std7_59.f_get_table_schema(p_table := p_table);
	v_main_table = std7_59.f_get_table_name(p_table := p_table);
	v_pxf_table = 'gp.'||v_main_table;

	-- подключение по pxf и выведение строки в консоль
	v_pxf = 'pxf://'||v_pxf_table||'?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.000:5432/postgres&USER='
					||p_user_id||'&PASS='||p_pass;
				
	select c.oid  																	-- ключи распределения целевой таблицы из системных таблиц
	into v_table_oid                                                                -- Идентификатор объекта (Object Identifier, OID) используется внутри Postgres Pro 
	from pg_class as c inner join pg_namespace as n on c.relnamespace = n.oid       -- в качестве первичного ключа различных системных таблиц
	where n.nspname||'.'||c.relname = p_table
	limit 1;

	if v_table_oid = 0 or v_table_oid is null then v_dist_key = 'distributed randomly';  -- получение ключа
	else
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	end if;
    -- coalesce возвращает первый элемент списка не равный NULL
	-- array_to_string выводит элементы массива через заданный разделитель и позволяет определить замену для значения NULL
	select coalesce('with (' || array_to_string(reloptions, ', ') || ')', '') -- запись в v_params параметры из целевой таблицы
	from pg_class 
	into v_params
	where oid = p_table::regclass;

	 -- удаление внешней таблицы
    execute 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table;

	-- создание внешней таблицы 
   if v_main_table = 'traffic'
   then
   		  v_sql = 'create external table '||v_ext_table||'(plant bpchar, date bpchar, time bpchar, frame_id bpchar, quantity int4)
			LOCATION ('''||v_pxf||'''
             ) ON ALL
			FORMAT ''CUSTOM'' ( FORMATTER=''pxfwritable_import'')
			ENCODING ''UTF8''';
		
		raise notice 'EXTERNAL TABLE IS: %', v_sql;   
		execute v_sql;
		
				v_sql = 'drop table if exists '||v_temp_table_2||';
						create table '||v_temp_table_2||' (like '||p_table||') '||v_params||' '||v_dist_key||';';
		
		execute v_sql;
		
				v_sql = 'insert into '||v_temp_table_2||'
						select plant, to_date(date, ''DD.MM.YYYY'') date, time, frame_id, quantity from '||v_ext_table||';';
	
		raise notice 'TEMP TABLE IS: %', v_sql;
		execute v_sql;     

	else
		v_sql = 'create external table '||v_ext_table||'(like '||p_table||')
			LOCATION ('''||v_pxf||'''
             ) ON ALL
			FORMAT ''CUSTOM'' ( FORMATTER=''pxfwritable_import'')
			ENCODING ''UTF8''';

		raise notice 'EXTERNAL TABLE IS: %', v_sql;   
		execute v_sql;
	end if;

	-- временные переменные
	v_load_interval = '1 month'::INTERVAL;                                  -- загружаемый периуд
	v_start_date := date_trunc('month', p_start_date);
	v_end_date := date_trunc('month', p_start_date) + v_load_interval;
		
	-- цикл для временных партиций
	WHILE v_start_date < p_end_date loop
		
		-- условие для выборки данных из внешней таблицы ключ партиционирования должен быть в пределах дат
		-- загружаемого периуда
		v_where = p_partition_key ||' >= '''||v_start_date||'''::date and '||p_partition_key||' < '''||v_end_date||'''::date';
		
		-- пересоздание временной таблицы
		v_sql = 'DROP TABLE IF EXISTS '|| v_temp_table ||';
				CREATE TABLE '|| v_temp_table ||' (LIKE '||p_table||') ' ||v_params||' '||v_dist_key||';';
	
		raise notice 'TEMP TABLE IS: %', v_sql;
		execute v_sql;
	
			if v_main_table = 'traffic'
			then
				v_sql = 'insert into '||v_temp_table||'
						select * from '||v_temp_table_2||'
						where '||v_where;
				execute v_sql;
			else
				v_sql = 'insert into '||v_temp_table||'
						select * from '||v_ext_table||'
						where '||v_where;
				execute v_sql;
			end if;
		
		get diagnostics v_cnt = row_count;
		raise notice 'INSERTED ROWS: %', v_cnt;
	
	--проверка наличия партиций у таблицы
	select count(*)
	into v_check_partition
	from (
	select tablename, partitiontablename, partitionrangestart, partitionrangeend
	from pg_partitions
	where schemaname = v_schema_name) as n
	where tablename = v_main_table and partitionrangestart = ''''||v_start_date||'''::date';

	if v_check_partition = 0
	then
		v_sql = 'alter table '||p_table||' SPLIT DEFAULT PARTITION START ('''||v_start_date||''') and ('''||v_end_date||''') exclusive';
		execute v_sql;
		
		v_sql = 'alter table '||p_table||' EXCHANGE PARTITION FOR (date '''||v_start_date||''') with table '||v_temp_table||' with validation';
		execute v_sql;
		
	else
		v_sql = 'alter table '||p_table||' EXCHANGE PARTITION FOR (date '''||v_start_date||''') with table '||v_temp_table||' with validation';
		execute v_sql;
	end if;

	v_result = v_result + v_cnt;
	
	v_start_date := v_start_date + v_load_interval;
	v_end_date   := v_end_date + v_load_interval;
	
	raise notice 'START_DATE: %', v_start_date;
	raise notice 'END_DATE: %', v_end_date;
		
	end loop;  

	perform std7_59.f_analyze_table(p_table_name := p_table);
	
	execute 'DROP TABLE IF EXISTS '||v_temp_table;
	execute 'DROP TABLE IF EXISTS '||v_temp_table_2;	

	return v_result;
		
end;
$$
execute on any;


select std7_59.f_load_delta_partition('std7_59.traffic', 'date', '2021-01-01', '2021-03-01', '', '');

select std7_59.f_load_delta_partition('std7_59.bills_item', 'calday', '2021-01-01', '2021-03-01', '', '');

select count(*) from std7_59.traffic;

select * from std7_59.bills_item;


--функция для расчета витрины

create or replace function std7_59.f_load_mart_rep(p_date_from text, p_date_to text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$

declare
	v_date_from 	date;
	v_date_to		date;
	v_table_name	text;
	v_sql			text;
	v_return		int;
	v_where		  	text;

begin
	
	v_table_name = 'std7_59.s_rep_' || to_char(p_date_from::timestamp, 'YYYYMMDD')|| '_' ||to_char(p_date_to::timestamp, 'YYYYMMDD');
	
	v_date_from	= to_date(p_date_from, 'YYYY-MM-DD');    			
	v_date_to = to_date(p_date_to, 'YYYY-MM-DD');
	v_where = 'BETWEEN '''||v_date_from||''' AND '''||v_date_to||'''';

	RAISE notice 'SQL_IS: %', v_date_from;
	RAISE notice 'SQL_IS: %', v_date_to;
	
	v_sql = 'drop table if exists '||v_table_name;
	execute v_sql;

	v_sql = 'create table '||v_table_name||
		' with (
				appendonly = true,
				orientation = column,
				compresstype = zstd,
				compresslevel = 1)
				as 
			with cte as (
			SELECT 
				ct.plant,
	    		ct.calday,
	    		SUM(ct.discount_amount) AS sum_discount,
	    		COUNT(*) AS qty_dics_pr
		  	FROM (SELECT 
			  	bi.billnum,
			    bi.material,
			    c.plant,
			    bi.calday,
			    p.promo_amount,
		    CASE 
			    WHEN p.promo_type = ''001'' THEN p.promo_amount
		      	WHEN p.promo_type = ''002'' THEN p.promo_amount * bi.rpa_sat * 0.01 / bi.qty
		    	ELSE 0 
				END AS discount_amount,
		   		ROW_NUMBER() OVER (PARTITION BY c.id_promo ORDER BY c.id_promo) AS rn
			FROM std7_59.coupons c
		    	JOIN std7_59.bills_item bi ON c.billnum = bi.billnum AND c.material = bi.material
		    	JOIN std7_59.promos p ON c.id_promo = p.id_promo
				where bi.calday '||v_where||') as ct
		  		WHERE rn = 1
		  		GROUP BY ct.plant, ct.calday),
			bills as (
			SELECT
				bi.calday,
				bh.plant,
		  		SUM(bi.rpa_sat) AS rpa_sum,
		    	SUM(bi.qty) AS qty_sum,   
		    	COUNT(DISTINCT bi.billnum) AS bills_count
		 	FROM std7_59.bills_item bi
		  		JOIN std7_59.bills_head bh ON bi.billnum = bh.billnum
				--where bi.calday '||v_where||'
		   		GROUP BY bh.plant, bi.calday),
			traffic_t as (
			select 
				plant, 
				"date", 
				sum(quantity) tr_d 
			from std7_59.traffic t
				where t."date" '||v_where||'
				group by plant, "date")
			select
				t.plant,																			--Завод
				s.txt,																			 	--Завод
				sum(b.rpa_sum) as rev,																--Оборот
				sum(cte.sum_discount) as disc,														--Скидки по купонам
				sum(b.rpa_sum) - sum(cte.sum_discount) as rev_with_disc,							--Оборот с учетом скидки
				sum(b.qty_sum) as qty,																--Кол-во проданных товаров
				sum(b.bills_count) as qty_bills,													--Количество чеков
				sum(t.tr_d) as traffic,																--Трафик
				sum(cte.qty_dics_pr) as sum_qty_dics_pr,											--Кол-во товаров по акции
				round(sum(cte.qty_dics_pr) / sum(b.qty_sum) * 100, 2) as prop_disc_prod,			--Доля товаров со скидкой
				round(sum(b.qty_sum) / sum(b.bills_count), 2) as avg_per_bill,						--Среднее количество товаров в чеке
				round(sum(b.bills_count) / sum(t.tr_d), 2) * 100 as conve_rate,						--Коэффициент конверсии магазина, %
				round(sum(b.rpa_sum) / sum(b.bills_count), 2) as avg_bill,							--Средний чек
				case
					when sum(t.tr_d) = 0 then 0
					else round(sum(b.rpa_sum) / sum(t.tr_d), 2)
				end as avg_rev_client																--Средняя выручка на одного посетителя
			from traffic_t t left join cte on t.plant = cte.plant and cte.calday = t."date"
				left join bills b on b.plant = t.plant and b.calday = t."date"
				left join stores s on s.plant = t.plant
				group by t.plant, s.txt, disc
			distributed randomly;';
		
	raise notice 'LOAD_MART TABLE IS: %', v_sql;
		
	execute v_sql;
	
	perform std7_59.f_analyze_table(p_table_name := v_table_name);
	
	
	execute 'select count(1) from ' ||v_table_name into v_return;

	return v_return;
	
end;
$$
EXECUTE ON ANY;

select * from std7_59.coupons c;

-- рассчет витрины
create or replace function std7_59.f_load_mart_fin(p_date_from text, p_date_to text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
declare
	v_date_from 	date;
	v_date_to		date;
	v_table_name	text;
	v_sql			text;
	v_return		int;
	v_where		  	text;

begin
	v_table_name = 'std7_59.s_fin_rep_' || to_char(p_date_from::timestamp, 'YYYYMMDD')|| '_' ||to_char(p_date_to::timestamp, 'YYYYMMDD');
	
	v_date_from	= to_date(p_date_from, 'YYYY-MM-DD');    			
	v_date_to = to_date(p_date_to, 'YYYY-MM-DD');
	v_where = 'BETWEEN '''||v_date_from||''' AND '''||v_date_to||'''';

	RAISE notice 'SQL_IS: %', v_date_from;
	RAISE notice 'SQL_IS: %', v_date_to;
	
	v_sql = 'drop table if exists '||v_table_name;
	execute v_sql;

	v_sql = 'create table '||v_table_name||
	' with (
			appendonly = true,
			orientation = column,
			compresstype = zstd,
			compresslevel = 1)
			as
	with rev_bill_qty as ( 										
	select plant,
		sum(rpa_sat) as rev,									--Оборот
		sum(qty) as mat_qty,									--Кол-во проданных товаров
		count(distinct(bi.billnum)) as bill_qty					--Количество чеков
	from bills_item bi join bills_head bh on bi.billnum = bh.billnum 
	where bi.calday '||v_where||' 
	group by plant
	),
	promo_coup_mat_qty as (									
	select 
		plant,
		sum(case 
			when promo_type = ''001'' then promo_amount
			when promo_type = ''002'' then cast(promo_amount as decimal) / 100 * (rpa_sat/qty)
			else 0
			end) 		as coupon_calc,							--Скидки по купонам
		count(c.id_promo) as coup_qty							--кол-во товаров по акции
	from (
	select distinct 
		c.plant, 
		c.c_num,    
		c.id_promo, 
		p.promo_type, 
		p.promo_amount, 
		bi.rpa_sat, 
		bi.qty 
	from coupons as c
	left join bills_item as bi
	on c.billnum = bi.billnum and c.material = bi.material 
	inner join promos as p
	on c.id_promo = p.id_promo and c.material = p.material
	where bi.calday '||v_where||' ) as c
	group by plant
	),
	traffics as (	
	select 
		plant, 
		sum(quantity) as traffic
	from traffic t
	where t."date" '||v_where||'
	group by plant
	)
	select 
		rbq.plant,
		rbq.rev,
		pcq.coupon_calc,
		(rbq.rev - pcq.coupon_calc) as trnvr_w_disc,
		rbq.mat_qty,
		rbq.bill_qty,
		t.traffic,
		pcq.coup_qty,
		cast(pcq.coup_qty * 100 / rbq.mat_qty as decimal(7,1)) as perc_promo_mat,
		cast(rbq.mat_qty / rbq.bill_qty as decimal(7,2)) as avg_mat_in_bill,
		cast(rbq.bill_qty * 100 / t.traffic::float as decimal(10,2)) as koef_conve,
		cast(rbq.rev / rbq.bill_qty as decimal(7,2)) as avg_bill,
		case
			when t.traffic = 0 then 0
			else cast(rbq.rev / t.traffic as decimal(10,1)) 
			end as rev_by_client
	from rev_bill_qty rbq left join promo_coup_mat_qty pcq on rbq.plant = pcq.plant
		right join traffics t on rbq.plant = t.plant 
	distributed randomly;';

	
	raise notice 'LOAD_MART TABLE IS: %', v_sql;
		
	execute v_sql;
	
	perform std7_59.f_analyze_table(p_table_name := v_table_name);
	
	
	execute 'select count(1) from ' ||v_table_name into v_return;

	return v_return;
	
end;
$$
EXECUTE ON ANY;



select std7_59.f_load_mart_fin('2021-01-01', '2021-01-01');

select std7_59.f_load_mart_fin('2021-01-01', '2021-02-28');

select std7_59.f_load_mart_rep('2021-01-01', '2021-02-28');

select * from std7_59.s_fin_rep_20210101_20210101;

select * from std7_59.s_fin_rep_20210101_20210110;

select * from std7_59.s_fin_rep_20210101_20210228;

select * from std7_59.s_rep_20210101_20210101;

select * from std7_59.s_rep_20210101_20210228;

select count(*) from std7_59.traffic;

select * from std7_59.bills_item order by calday desc;

select count(*) from std7_59.bills_head;

select count(*) from std7_59.bills_item_ext;

select * from std7_59.bills_item where calday between '2021-01-01' and '2021-02-28';

select count(*) from std7_59.bills_item_ext where calday between '2021-01-01' and '2021-02-28';

select count(*) from std7_59.bills_head where calday between '2021-01-01' and '2021-02-28';

select * from std7_59.bills_head_ext where calday between '2021-01-01' and '2021-02-28';
