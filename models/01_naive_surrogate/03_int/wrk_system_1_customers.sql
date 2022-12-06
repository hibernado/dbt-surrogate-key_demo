{{ config(
    materialized='table'
    ) }}

with customers as (
    select *
    from {{ ref('stg_system_1_customers') }}
)
, watermark as (

    select max(watermark::int) w
    from {{ ref('watermarks')}}
    where source_table_name = '{{ ref('stg_system_1_customers').identifier }}'

)

select *
from customers
where incremental_counter > (select w from watermark)