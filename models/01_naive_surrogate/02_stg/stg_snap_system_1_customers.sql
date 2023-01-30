{{ config(
    materialized='view',
    unique_id='customer_id_snapshot_id'
) }}

with customers as (
    select *
        ,{{ dbt_utils.generate_surrogate_key([
            'customer_id',
            'row_timestamp']) }} as customer_id_snapshot_id
    from {{ ref('src_system_1_customers') }}
)

select
    {{ dbt_utils.star(ref('src_system_1_customers')) }}
    ,customer_id_snapshot_id
from (

    select *
        ,row_number() over (
            partition by customer_id_snapshot_id
            order by incremental_counter desc) as rnk
    from customers
) b where rnk = 1
