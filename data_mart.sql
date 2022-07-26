create or replace view dm_revenue_v as
with 
task_is_date as 
(
    select issue_date                    -- запрос по заданию 1 на календарную дату
      from calendar_dates
),
city_code_name_cte as 
(
    select city_code,                    -- запрос на табл. city_code_name
           city_name,
           code_name,
           valid_from_dttm,
           valid_to_dttm
      from city_code_name
),
name_town as 
(
    select distinct 
                   city_name,           -- название города и его код
                   city_code
      from city_code_name_cte
),
list_town_trx as 
(
    select name_town.city_name,          -- название города, кодовое название города, дата транзакции, сумма транзакций
           ccn.code_name,
           tr.trx_date,
           sum(case when tr.payment_type in ('Штрафы', 'Возврат')
                     then tr.total_amount * -1 else tr.total_amount end) as trx
      from customer_transactions tr
 left join name_town
        on tr.city_code = name_town.city_code
 left join city_code_name_cte ccn 
        on name_town.city_code = ccn.city_code 
     where tr.trx_date between ccn.valid_from_dttm and ccn.valid_to_dttm
  group by name_town.city_name, tr.trx_date, ccn.code_name
  order by trx_date
),
task_list_names_revenue as 
( 
    select task_is_date.issue_date,                       -- список по заданию 2,3 и 4, дата, города в ряд, кодовые названия в ряд, выручка за день
           nvl(listagg(list_town_trx. city_name, ';')
               within group (order by list_town_trx. city_name), 'NONE') "ISSUE_CITY_NAMES",
           nvl(listagg(list_town_trx. code_name, ';')
               within group (order by list_town_trx. city_name), 'NONE') "ISSUE_CITY_CODE_NAMES",
           nvl(sum(list_town_trx.trx), 0) as revenue_amount_day
      from task_is_date
 left join list_town_trx 
        on task_is_date.issue_date = list_town_trx.trx_date
  group by task_is_date.issue_date
  order by task_is_date.issue_date
),
task_amount_month as 
(
    select distinct c.issue_date,                --по заданию 5, сумма выручки за месяц
           sum(case when tr.payment_type in ('Штрафы','Возврат')
                     then tr.total_amount * -1 else tr.total_amount
               end) 
           over (partition by (extract (month from c.issue_date) || '/' || extract (year from c.issue_date))) as revenue_amount_month_plan
      from customer_transactions tr
right join calendar_dates c
        on tr.trx_date = c.issue_date
  order by c.issue_date
),
unpivot_payment_total_month as 
(
    select to_date((month || '-' || year), 'MON-YYYY') "M_DATE",
           revenue_amount_month_fact,
           balance_debt
      from payment_total_month
   unpivot (revenue_amount_month_fact for month in (JANUARY, FEBRUARY, MARCH, APRIL,MAY,JUNE,JULY,AUGUST,SEPTEMBER,OCTOBER,NOVEMBER,DECEMBER)) 
  order by "M_DATE"
),
main_query as 
(
    select task_list_names_revenue.issue_date,
           task_list_names_revenue.issue_city_names,
           task_list_names_revenue.issue_city_code_names,
           task_list_names_revenue.revenue_amount_day,
           tam.revenue_amount_month_plan,
           upt.revenue_amount_month_fact,
           upt.revenue_amount_month_fact - tam.revenue_amount_month_plan as plan_fact_correction,
           lag (upt.revenue_amount_month_fact, 30, upt.balance_debt) over (order by m_date) as revenue_amount_month_fact_prev,
           sum(distinct upt.revenue_amount_month_fact) over(partition by extract (year from task_list_names_revenue.issue_date)) as revenue_amount_year,
           case 
              when (TO_CHAR(task_list_names_revenue.issue_date, 'FmDay')) in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday') and task_list_names_revenue.revenue_amount_day < = 0 then 'ПЛОХОЙ ДЕНЬ'
              when (TO_CHAR(task_list_names_revenue.issue_date, 'FmDay')) in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday') and task_list_names_revenue.revenue_amount_day > 0 then 'ХОРОШИЙ ДЕНЬ'
              when (TO_CHAR(task_list_names_revenue.issue_date, 'FmDay')) in ('Saturday', 'Sunday') 
                then 'ВЫХОДНОЙ'       
           end as additional_information
      from task_list_names_revenue
inner join task_amount_month tam
        on task_list_names_revenue.issue_date = tam.issue_date
 left join unpivot_payment_total_month upt
       on (extract (month from task_list_names_revenue.issue_date) || '/' || extract (year from task_list_names_revenue.issue_date)) = (extract (month from upt.M_DATE) || '/' || extract (year from upt.M_DATE))
  order by task_list_names_revenue.issue_date
 )
    select issue_date,
           issue_city_names,
           issue_city_code_names,
           revenue_amount_day,
           revenue_amount_month_plan,
           revenue_amount_month_fact,
           plan_fact_correction,
           revenue_amount_month_fact_prev,
           revenue_amount_year,
           additional_information
      from main_query
  order by issue_date;
