{{ config(
    materialized='view',
    unique_id='customer_id'
) }}

with customers as (
    select *
    from {{ ref('src_system_1_customers') }}
)

select
    {{ dbt_utils.star(ref('src_system_1_customers')) }}
from (

    select *
        ,row_number() over (
            partition by customer_id
            order by row_timestamp desc) as rnk
    from customers
) b where rnk = 1
