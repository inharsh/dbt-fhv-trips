{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    dispatch_no,
    pickup_locationid,
    dropoff_locationid,
    affiliated_no,
    sr_flag,
    pickup_datetime,
    dropoff_datetime

from {{ ref('stg_fhv_tripdata') }} as stg_fhv

inner join dim_zones as pickup_zone
    on stg_fhv.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on stg_fhv.dropoff_locationid = dropoff_zone.locationid