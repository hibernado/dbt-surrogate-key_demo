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
    from {{ ref('stg_snap_system_1_customers') }}
)
, watermark as (

    select max(watermark::int) w
    from {{ ref('watermarks')}}
    where target_table_name = '{{ this.identifier }}'

)

select
    o.*
    ,o.row_timestamp as scd_valid_from
    ,coalesce(
        lead(o.row_timestamp) over (
            partition by o.customer_id
            order by o.row_timestamp
        )
    ,'2099-12-31 23:59'::timestamp ) as scd_valid_to
from customers o
where o.customer_id in (
    select distinct customer_id
    from customers n
    where n.incremental_counter > (select w from watermark)
)
