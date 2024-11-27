-- решение тестового задания



--1. Идентификаторы всех дат с более высокими температурами по сравнению с предыдущими датами.
select wt.id
from weather ytw cross join weather wt
where wt.recordDate - ytw.recordDate = 1		-- учитывабтся даты которые идут подряд
    and wt.temperature > ytw.temperature;		-- выше ли сегодняшняя температура чем вчера
    
    
--2. Поиск клиента с наибольшим кол-м заказов(только один клиент разместил наибольшее кол-во заказов)
select customer_number
from orders
group by customer_number
order by count(*) desc
limit 1;


--3. Поиск второй по величине зарплаты. Если ее нет вывести null
SELECT
    COALESCE(
    (SELECT
        salary
    FROM
        (SELECT
            *,
            DENSE_RANK() OVER(ORDER BY salary DESC) as rn
        FROM
            Employee)
    WHERE
        rn = 2
    LIMIT 1), NULL
    )  AS SecondSalary;
    
--4. Выберите ID товара с самым большим количеством заказов с разбивкой по годам
with count_ord as (											--формирование года
  select 
  to_char(ord_datetime, 'yyyy') as year,
  an_id,
  count(ord_an) as cnt
  from orders o join analysis an on o.ord_an = an.an_id
  group by 1, 2
),
ranck as (													--нумерация по годам
  select *,
  rank() over(partition by year order by cnt desc) as rn
  from count_ord
)
select 														
year,
an_id,
cnt
from ranck
where rn = 1
order by year, an_id;

--5. Нарастающим итогом рассчитать, как увеличивалось количество проданных тестов каждый месяц каждого года с разбивкой по группе
with change_str as (                										--подсчет в оконной функции кол-во проданных тесто
select																		--с разбивкой по группе и году
year,
month,
"group",
cast(sum(cn) over (partition by "group", year  ORDER by month rows between unbounded preceding and current row) as int) as calc
from (
SELECT
to_char(ord_datetime, 'YYYY') as year,
--to_char(ord_datetime, 'month') as month,
EXTRACT(month FROM ord_datetime) AS month,
gr_id as group,
count(ord_id) as cn
from orders o join analysis an on o.ord_an = an.an_id
  join groups g on g.gr_id = an.an_group
group by 3, 1, 2
order by 3, 1, 2) as foo
order by "group", year, month
)
select 
year,
LPAD(month::text, 2, '0') as month,
"group",
calc as "sum" 
from change_str;
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   