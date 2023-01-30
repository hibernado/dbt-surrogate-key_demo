{{ config(
    materialized='incremental',
    unique_key='skey_customer_id_snapshot_id',
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
    from {{ ref('int_dim_customers_scd') }}
)


select
    coalesce(
        {% if is_incremental() %} dim.skey_customer_id_snapshot_id {% else %} null {% endif %}
        ,{{ dbt_utils.generate_surrogate_key(['stg.customer_id_snapshot_id']) }}
    ) as skey_customer_id_snapshot_id
    ,stg.*
    ,{% if is_incremental() %}
     case when dim.customer_id is null then 'insert' else 'upsert' end
     {% else %} 'insert'
     {% endif %} as dbt_action
    ,now()::timestamp as dbt_timestamp
from customers stg


{% if is_incremental() %}
left join {{ this }} dim
    on stg.customer_id_snapshot_id = dim.customer_id_snapshot_id
{% endif %}