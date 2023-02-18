{{ config(materialized='view') }}

select
-- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num as  dispatch_no,
    cast(PULocationID as integer) pickup_locationid,
    cast(DOLocationID as integer) dropoff_locationid,
    Affiliated_base_number as  affiliated_no,
    cast(SR_Flag as numeric) sr_flag,

-- timestamps
    cast(pickup_datetime as timestamp) pickup_datetime,
    cast(dropoff_datetime as timestamp) dropoff_datetime,

from {{ source('staging', 'fhv_tripdata') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}