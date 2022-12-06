{{ config(
    materialized='incremental'
    ) }}

with customers as (
    select *
    from {{ ref('system_1_customers') }}
)

select *
from customers

{% if is_incremental() %}
where incremental_counter = (
    select max(incremental_counter) + 1 from {{ this }})
{% else %}
    where incremental_counter = 1
{% endif %}
