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
    from {{ ref('wrk_system_1_customers') }}
)


select
    coalesce(
        {% if is_incremental() %} dim.skey_customer_id {% else %} null {% endif %}
        ,{{ dbt_utils.surrogate_key(['stg.customer_id']) }}
    ) as skey_customer_id
    ,stg.*
    ,{% if is_incremental() %}
     case when dim.customer_id is null then 'insert' else 'upsert' end
     {% else %} 'insert'
     {% endif %} as t_action
from customers stg


{% if is_incremental() %}
left join {{ this }} dim
    on stg.customer_id = dim.customer_id
{% endif %}