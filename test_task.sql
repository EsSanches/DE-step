-- средняя посещаемость по каждой из тренировок
select 
round((select count(distinct customer_id) from std10_68.test_task where is_attend = 1) / count(event_id), 1) as cnt,
event_id
from test_task
where is_attend = 1
group by event_id
order by event_id;

-- id посетителе которые больше всего пропустили зантий
select customer_id
from (
SELECT 
count(customer_id) as cnt_cus,
customer_id,
rank() over (order by cnt_cus desc) as rn
from test_task
where is_attend = 0
group by customer_id
order by count(customer_id) desc)
where rn = 1;


-- учителя с самым большим посещение по месяцам
select 
mm,
teacher_ids,
cnt_c
from (
select *,
rank() over(partition by mm order by cnt_c desc) as rn
from (
select 
count(customer_id) as cnt_c,
teacher_ids,
toMonth(event_date) as mm         -- posgresql to_char(event_date, month)
from test_task
group by teacher_ids, event_date))
where rn = 1;


-- уроки с максимальным кол-вом посетителей
select 
count(event_id) as c_e,
event_id
from std10_68.test_task
group by event_id
having c_e = (SELECT 
max(c_e) as m_cnt
from(
select 
count(event_id) as c_e,
event_id
from test_task
group by event_id));