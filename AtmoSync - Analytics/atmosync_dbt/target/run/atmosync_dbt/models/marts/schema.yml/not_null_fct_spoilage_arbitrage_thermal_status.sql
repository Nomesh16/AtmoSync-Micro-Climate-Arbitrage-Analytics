
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select thermal_status
from ATMOSYNC_DB.ANALYTICS.fct_spoilage_arbitrage
where thermal_status is null



  
  
      
    ) dbt_internal_test