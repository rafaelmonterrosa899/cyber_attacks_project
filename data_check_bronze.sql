

select *
from workspace.bronze.financial_impact_raw
limit 100;

select *
from workspace.bronze.incidents_master_raw
limit 100; 

select *
from workspace.bronze.market_impact_raw
limit 100;


-- Row counts
SELECT 'incidents_master_raw' AS table_name, COUNT(*) AS rows
FROM workspace.bronze.incidents_master_raw
UNION ALL
SELECT 'financial_impact_raw', COUNT(*)
FROM workspace.bronze.financial_impact_raw
UNION ALL
SELECT 'market_impact_raw', COUNT(*)
FROM workspace.bronze.market_impact_raw;

-- Null incident_id checks
SELECT 'incidents_master_raw' AS table_name, COUNT(*) AS null_incident_id
FROM workspace.bronze.incidents_master_raw
WHERE incident_id IS NULL
UNION ALL
SELECT 'financial_impact_raw', COUNT(*)
FROM workspace.bronze.financial_impact_raw
WHERE incident_id IS NULL
UNION ALL
SELECT 'market_impact_raw', COUNT(*)
FROM workspace.bronze.market_impact_raw
WHERE incident_id IS NULL;

-- Duplicates by incident_id (key health)
SELECT 'incidents_master_raw' AS table_name,
       COUNT(*) - COUNT(DISTINCT incident_id) AS dup_incident_id
FROM workspace.bronze.incidents_master_raw
UNION ALL
SELECT 'financial_impact_raw',
       COUNT(*) - COUNT(DISTINCT incident_id)
FROM workspace.bronze.financial_impact_raw
UNION ALL
SELECT 'market_impact_raw',
       COUNT(*) - COUNT(DISTINCT incident_id)
FROM workspace.bronze.market_impact_raw;
