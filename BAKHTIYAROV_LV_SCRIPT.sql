drop table task_1;
drop table task_2_mid;
drop table task_2;
drop table task_3;
drop table task_4_window;
drop table task_4_group;
drop table task_5_max;
drop table task_5_min;
drop table task_6;
drop table task_7;



-- TASK 1
----------------------------------
-- Предполагаю, что n/a тоже стоит учитывать

select count(job_industry_category)
	, job_industry_category into task_1
from customer
group by job_industry_category
order by count(job_industry_category) desc;


-- TASK 2
----------------------------------

select *, replace(list_price, ',', '.') into task_2_mid
from transaction
where list_price <> '';

alter table task_2_mid drop column list_price;
alter table task_2_mid rename column replace to list_price;
alter table task_2_mid alter column list_price set not null;

alter table task_2_mid alter column list_price type float4 using list_price::float4;


select date_trunc('month', transaction_date::date) as date_month 
	, job_industry_category
	, sum(list_price) as sum_price into task_2
from task_2_mid
inner join customer on task_2_mid.customer_id = customer.customer_id
group by job_industry_category, date_trunc('month', transaction_date::date)
order by date_month desc, sum_price desc;


-- TASK 3
----------------------------------
-- Предполагаю, что оставляем пустые бренды, чтобы можно было установить, 
-- сколько транзакций прошло без упоминания бренда
-- (в крайнем случае, добавить последним условие brand <> '' или настроить is not null на стадии выгрузки данных)

select count(brand) as count_brand
	, brand
	, online_order
	, order_status
	, job_industry_category into task_3
from transaction
inner join customer on transaction.customer_id = customer.customer_id
where  
	online_order = true and 
	order_status = 'Approved' and 
	job_industry_category = 'IT'
group by brand, online_order, order_status, job_industry_category
order by count(brand) desc;


-- TASK 4
----------------------------------

-- Оконные функции
select 
	customer_id
	, sum(list_price) over (partition by customer_id) as sum_price
	, count(customer_id) over (partition by customer_id) as count_customer
	, max(list_price) over (partition by customer_id) as max_price
	, min(list_price) over (partition by customer_id) as min_price into task_4_window
from task_2_mid
order by count_customer desc, sum_price desc;

-- Использование group by
select 
	customer_id
	, sum(list_price) as sum_price
	, count(customer_id) as count_customer
	, max(list_price) as max_price
	, min(list_price) as min_price into task_4_group
from task_2_mid
group by customer_id 
order by count_customer desc, sum_price desc;

	
-- TASK 5
----------------------------------
-- По идее, если за весь период, то нет смысла настраивать диапазон дат
-- Оставляю индекс пользователей, чтобы для тех, у кого совпадают имена, было видно, что это разные пользователи


-- Для максимума
select
	distinct customer.customer_id
	, first_name
	, last_name
	, list_price into task_5_max
from task_2_mid
inner join customer on task_2_mid.customer_id = customer.customer_id
where list_price = (select max(list_price) from task_2_mid)
order by customer_id desc;

-- Для минимума
select
	distinct customer.customer_id
	, first_name
	, last_name
	, list_price into task_5_min
from task_2_mid
inner join customer on task_2_mid.customer_id = customer.customer_id
where list_price = (select min(list_price) from task_2_mid)
order by customer_id desc;


-- TASK 6
----------------------------------

select 
	customer_id
	, first_value(transaction_id) over (partition by  customer_id order by date_trunc('day', transaction_date::date) asc) as first_transaction
	, first_value(date_trunc('day', transaction_date::date)) over 
		(partition by customer_id order by date_trunc('day', transaction_date::date) asc) as first_date into task_6
from task_2_mid;


-- TASK 7
----------------------------------
-- Оставляю индекс пользователей, чтобы для тех, у кого совпадают имена, было видно, что это разные пользователи

with task_7_mid_1 as (
	select 
		task_2_mid.customer_id
		, first_name
		, last_name
		, job_title
		, first_value(date_trunc('day', transaction_date::date)) over 
			(partition by task_2_mid.customer_id order by date_trunc('day', transaction_date::date) asc) as first_date
		, first_value(date_trunc('day', transaction_date::date)) over 
			(partition by task_2_mid.customer_id order by date_trunc('day', transaction_date::date) desc) as last_date
	from task_2_mid
	inner join customer on task_2_mid.customer_id = customer.customer_id
),
	task_7_mid_2 as (
	select
		distinct customer_id
		, first_name
		, last_name
		, job_title
		, last_date - first_date as difference_date
	from task_7_mid_1
)
select * into task_7
from task_7_mid_2
where difference_date = (select max(difference_date) from task_7_mid_2)



