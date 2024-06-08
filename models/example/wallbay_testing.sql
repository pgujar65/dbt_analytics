{{ config(materialized='table') }}

WITH sr_detail AS 
(	
select * from RAW_DATA.in_wallbay_sr_detail
where civil != "CSD"
),
wallbayScanReportData as (
select * from RAW_DATA.in_wallbay_scan_zylem
),
srDetailDataSelected as 
( 
select distinct b.region,a.state,a.city,a.sales_representative_name,a.asset_no,a.customer_id,a.licence_no,a.wallbay_description
FROM sr_detail a 
left join (select distinct region,state from wallbayScanReportData) b 
on a.state = b.state
),
srDetailAssign as (
select region,state,city,sales_representative_name,count(asset_no) as total_wb 
from srDetailDataSelected
group by 1,2,3,4
),
srDetailAssetJoin as 
(
select distinct * from srDetailAssign
join srDetailDataSelected
using(region,state,city,sales_representative_name)
),
wallbayScanDupsRemove as 
(
select *, row_number() over (partition by asset_no,sales_representative_name,asset_no,date order by time) as rw_no 
from wallbayScanReportData
),
wallbayScanDate as 
(
SELECT *,
     
    CASE
    WHEN EXTRACT(DAY FROM DATE(date)) BETWEEN 1 AND 8 THEN 'Week1'
    WHEN EXTRACT(DAY FROM DATE(date)) BETWEEN 9 AND 15 THEN 'Week2'
    WHEN EXTRACT(DAY FROM DATE(date)) BETWEEN 16 AND 22 THEN 'Week3'
    WHEN EXTRACT(DAY FROM DATE(date)) BETWEEN 23 AND 31 THEN 'Week4'
    ELSE 'Other'
  END AS week
FROM wallbayScanDupsRemove
where rw_no = 1 
),
wallbayReport as 
(
select distinct sr.region, sr.state, sr.city, sr.sales_representative_name,sr.licence_no,sr.wallbay_description, total_wb, asset_no, customer_id, date, time, latitude, longitude,remark,civil,week,
case when outlet is null then outlet_name else outlet end as outlet
from srDetailAssetJoin sr 
left join wallbayScanDate wb 
using(asset_no,customer_id,sales_representative_name)
left join (select distinct customer_id,outlet as outlet_name from wallbayScanDate)
using(customer_id)
)

select * from wallbayReport