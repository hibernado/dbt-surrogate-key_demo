{{ config(
    materialized='incremental',
    unique_key='skey_customer_id',
    post_hook=[
        after_commit("""
        insert into {{ ref('watermarks')}}
        (target_table_type,target_table_name,source_table_name, watermark, run_time)
        select
            'dimension'
            ,'{{ this.identifier }}'
            ,'{{ ref('stg_system_1_customers').identifier }}'
            ,max(incremental_counter)
            ,now()::timestamp
        from {{ this }}
        """
        )]
    ) }}

with customers as (
    select *
        ,now()::timestamp skey_seed
    from {{ ref('wrk_system_1_customers') }}
)


select
    {{ dbt_utils.generate_surrogate_key(['stg.customer_id', 'stg.skey_seed']) }} as skey_customer_id
    ,stg.customer_id
    ,stg.incremental_counter
    ,stg.favourite_colour
    ,stg.row_timestamp
    ,stg.row_timestamp as scd_valid_from
    ,'2099-12-31 23:59'::timestamp  as scd_valid_to
    ,'insert' as dbt_action
    ,now()::timestamp as dbt_timestamp
from customers stg
{% if is_incremental() %}

union all

select
     dim.skey_customer_id
    ,dim.customer_id
    ,dim.incremental_counter
    ,dim.favourite_colour
    ,dim.row_timestamp
    ,dim.scd_valid_from
    ,stg.row_timestamp as scd_valid_to
    ,'upsert' as dbt_action
    ,now()::timestamp as dbt_timestamp
from customers stg
join {{ this }} dim
    on stg.customer_id = dim.customer_id
    and dim.scd_valid_to = '2099-12-31 23:59'::timestamp

{% endif %}