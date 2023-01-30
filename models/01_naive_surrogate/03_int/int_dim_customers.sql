{{ config(
    materialized='table',
    post_hook=[
        after_commit("""
        insert into {{ ref('watermarks')}}
        (target_table_type,target_table_name, watermark, run_time)
        select
            'dimension'
            ,'{{ this.identifier }}'
            ,max(incremental_counter)
            ,now()::timestamp
        from {{ this }}
        """
        )]
    ) }}

with customers as (
    select *
    from {{ ref('stg_curr_system_1_customers') }}
)
, watermark as (

    select max(watermark::int) w
    from {{ ref('watermarks')}}
    where target_table_name = '{{ this.identifier }}'

)

select *
from customers
where incremental_counter > (select w from watermark)