
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select container_id
from ATMOSYNC_DB.ANALYTICS.fct_spoilage_arbitrage
where container_id is null



  
  
      
    ) dbt_internal_test