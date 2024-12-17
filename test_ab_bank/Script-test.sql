
-- количество уникальных посетителей, которые заходили на сайт из разных городов
select 
count(distinct user_id) as qnt_unique_users
from (
select 
user_id
from events
group by user_id
having count(distinct city) > 1);


-- город, с максимальным числом уникальных посетителей. Вывести в разрезе каждогo месяца
select
"month",
city
from (
select *,
rank() over(partition by "month" order by unique_visitors desc) as rn
from (
select 
to_char(event_datetime, 'month') as "month",
city,
count(distinct user_id) as unique_visitors 
from events
group by "month", city
ORDER BY month, unique_visitors desc) as co_u)
where rn = 1;


-- *среднее количество посещений сайта , потребовавшееся посетителям, чтобы принять
-- решение об открытии счета ( результат = одно число, только по посетителям, открывшим счет)
select 
round(avg(visit_count)) as avg_visit
from (
select user_id, 
count(*) as visit_count 
from events
where event_name = 'Посещение сайта'
	and user_id in (select distinct user_id from events where event_name = 'Открытие счета')
group by user_id);


-- *среднее число дней , потребовавшееся посетителям, чтобы принять решение об
-- открытии счета ( Результат = одно число, только по посетителям, открывшим счет, считаем с
-- первого посещения сайта)
select 
round(avg(date_opn_acc - first_visit)) as avg_day        --
from (
select user_id,
min(event_datetime) as first_visit,
max(case when event_name = 'Открытие счета' then event_datetime end) as date_opn_acc
from events
group by user_id)
where date_opn_acc is not null;


-- идентификаторы посетителей, которые открыли счет в день первого посещения сайта
select 
user_id
from (
select user_id,
min(event_datetime) as first_visit,
max(case when event_name = 'Открытие счета' then event_datetime end) as date_opn_acc
from events
group by user_id)
where date_opn_acc is not null and first_visit = date_opn_acc;


-- стоимость привлечения и количество посетителей, которые в течение года так и не открыли счет
select 
count(distinct user_id) as qnt_visitor,
sum(sc) as total_amount
from (
select 
user_id,
sum("cost") as sc,
to_char(min(event_datetime), 'yyyy')::integer as first_visit,
to_char(max(event_datetime), 'yyyy')::integer as last_visit
from events
where user_id not in (select user_id from events where event_name = 'Открытие счета')
group by 1) as c
where last_visit-first_visit <= 1;


-- стоимость затрат на посещения после открытия счета
select 
sum(e."cost") as total_sum
from events e 
where e.event_name = 'Посещение сайта' 
	and e.event_datetime > (
select 
min(event_datetime)
from events 
where event_name = 'Открытие счета'
	and user_id = e.user_id);


-- стоимость привлечения и количество посетителей, которые в течение года открыли счет
select 
count(distinct user_id) as qnt_visitor,
sum(sc) as total_amount
from (
select 
user_id,
sum("cost") as sc,
to_char(min(event_datetime), 'yyyy')::integer as first_visit,
to_char(max(event_datetime), 'yyyy')::integer as last_visit
from events
where user_id in (select user_id from events where event_name = 'Открытие счета')
group by 1) as c
where last_visit-first_visit <= 1;


-- уникальные даты посещения сайта посетителем, стоимость привлечения которого на
-- сайт (Событие "Посещение сайта", за весь год) оказалась самой высокой. Независимо от того,
-- открыл или не открыл счет
with CTE as (
select 
user_id,
sum("cost") as tot_c,
rank() over(order by sum("cost") desc) as rn
from events
group by 1
order by 2 desc
)
select 
distinct event_datetime 
from  events e join cte on e.user_id = CTE.user_id and rn = 1
where event_name = 'Посещение сайта';


-- ежемесячное изменение количества уникальных посетителей сайта
with m_visitor as (
select 
to_char(event_datetime, 'mm') as "month",
count(distinct user_id) as qnt_user
from events 
group by to_char(event_datetime, 'mm')
),
visitor_diff as (
select 
"month",
qnt_user,
lag(qnt_user) over(order by "month") ear_visitor
from m_visitor
)
select
"month", 
round(((qnt_user-ear_visitor)/ear_visitor::numeric) * 100)||'%' as percent_prev_manth
from visitor_diff;

