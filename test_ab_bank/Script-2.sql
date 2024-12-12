create schema test_task;

-- DDL
--создание таблиц

drop table if exists test_task.clients;
create table test_task.clients(
client_id int4 not null,
client_surname varchar(50) not null,
client_name varchar(50) not null,
client_second_name varchar(50) not null,
birth_date date not null,
constraint f_client_pkey primary key (client_id)
);

drop table if exists test_task.products;
create table test_task.products(
product_id int4 not null,
product_name varchar(50) not null,
product_group_name varchar(50) not null,
constraint f_product_pkey primary key (product_id)
);

drop table if exists test_task.application;
create table test_task.application(
id int4 not null,
product_id int4 not null,
client_id int4 not null,
app_date date not null,
app_value int8 not null,
constraint f_app_id_pkey primary key (id)
);

-- назначение зависимостей
alter table test_task.application add constraint f_client_pkey foreign key (client_id) references test_task.clients(client_id);
alter table test_task.application add constraint f_product_pkey foreign key (product_id) references test_task.products(product_id);


--решение задач

-- суммa заявок по месяцам и продуктам
select 
to_char(app_date, 'month') as "month",
p.product_name,
sum(a.app_value) AS total_value
from test_task.application a join test_task.products p using(product_id)
group by "month", p.product_name;

-- сумма и количество заявок по месяцам, продуктам, группам продуктов, где месяц заявки равен августу 2021 года.
select 
to_char(app_date, 'month') as "month",
count(*) as qnt_app,
p.product_name,
p.product_group_name,
sum(a.app_value) AS total_value
from test_task.application a join test_task.products p using(product_id)
where app_date between '2021-08-01' and '2021-08-31'
group by "month", p.product_name, p.product_group_name;
-- Execution Time: 0.179 ms

--сумма и количество заявок по месяцам, продуктам, группам продуктов и группам возвратов клиентов с шагом 10 лет
select 
to_char(app_date, 'month') as "month",
count(*) as qnt_app,
p.product_name,
p.product_group_name,
-- to_char выводим только год приводим к типу int и получаем год в виде тысч
-- дальше определяем дистаннцию в данном случае 10 лет(год включительно)
(to_char(c.birth_date, 'YYYY')::integer / 10) * 10 || '-' || (to_char(c.birth_date, 'YYYY')::integer / 10) * 10 + 9 as client_age_group,
sum(a.app_value) AS total_value
from test_task.application a join test_task.products p using(product_id)
	join test_task.clients c using(client_id)
group by "month", p.product_name, p.product_group_name, client_age_group;

-- суммa и количество заявок накопительным итогом по месяцам
select 
to_char(a.app_date, 'month') as "month",
sum(count(*)) over(partition by to_char(app_date, 'month') rows between unbounded preceding and current row) ac_qnt_app_res,
sum(a.app_value) over(partition by to_char(app_date, 'month') rows between unbounded preceding and current row) as ac_app_res
from test_task.application a join test_task.products p using(product_id)
group by a.app_date, a.app_value;

-- суммa и количество заявок накопительно по месяцам
select 
"month",
sum(total_count) over(order by "month") as ac_qnt_app_res,
sum(total_value) over(order by "month") as ac_app_res
from (
	select
	to_char(app_date, 'month') as "month",
	sum(app_value) as total_value,
	count(*) as total_count
	from test_task.application
	group by "month"
) as foo
order by "month";









