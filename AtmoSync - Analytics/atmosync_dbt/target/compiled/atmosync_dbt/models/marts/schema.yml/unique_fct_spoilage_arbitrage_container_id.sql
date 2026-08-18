
    
    

select
    container_id as unique_field,
    count(*) as n_records

from ATMOSYNC_DB.ANALYTICS.fct_spoilage_arbitrage
where container_id is not null
group by container_id
having count(*) > 1


