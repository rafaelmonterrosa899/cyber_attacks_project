

--This script will create a first version of the gold layer that unifies all the columns we need to run an analysis on python to find correlation between variables. 


-- drop table if exists workspace.gold.incidents_master_gold;

-- create table workspace.gold.incidents_master_gold as 


select 
    ma.incident_id
    ,ma.company_name
    ,ma.company_revenue_usd
    ,co.country_name
    ,ma.employee_count
    ,ma.is_public_company
    ,ma.incident_date
    ,ma.discovery_date 
    ,ma.disclosure_date
    ,ma.attack_vector_primary
    ,ma.attack_vector_secondary
    ,ma.data_compromised_records
    ,ma.data_type
    ,ma.systems_affected
    ,ma.downtime_hours
    ,fi.direct_loss_usd
    ,fi.ransom_demanded_usd
    ,fi.recovery_cost_usd
    ,fi.legal_fees_usd 
    ,fi.regulatory_fine_usd 
    ,fi.insurance_payout_usd 
    ,fi.total_loss_usd
    ,mi.price_7d_after 
    ,mi.price_7d_before 
    ,mi.days_to_price_recovery
   ,ROUND(fi.total_loss_usd / NULLIF(ma.company_revenue_usd,0), 4) AS total_loss_revenue_ratio
   ,ROUND(fi.total_loss_usd / NULLIF(ma.employee_count,0), 2) AS total_loss_employee_ratio
   ,ROUND(fi.total_loss_usd / NULLIF(ma.downtime_hours,0), 2) AS total_loss_downtime_ratio
   ,ROUND(fi.total_loss_usd / NULLIF(ma.data_compromised_records,0), 6) AS total_loss_data_compromised_records_ratio


from workspace.silver.incidents_master as ma 
left join workspace.gold.dim_country as co 
  on co.country_iso2 = ma.country_hq
left join workspace.silver.financial_impact as fi 
    on fi.incident_id = ma.incident_id
left join workspace.silver.market_impact as mi 
    on mi.incident_id = ma.incident_id 



