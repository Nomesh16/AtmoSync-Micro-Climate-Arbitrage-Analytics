
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select recommended_reroute_market
from ATMOSYNC_DB.ANALYTICS.fct_spoilage_arbitrage
where recommended_reroute_market is null



  
  
      
    ) dbt_internal_test