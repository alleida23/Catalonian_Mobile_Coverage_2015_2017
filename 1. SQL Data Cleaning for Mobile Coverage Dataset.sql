###################################################
## SQL Data Cleaning for Mobile Coverage Dataset ##
###################################################

# Purpose:

"This SQL code is designed to clean and prepare a public dataset of mobile coverage data in Google BigQuery. The primary goal is data cleaning, which includes creating a new table, normalizing categorical features, handling missing data, and removing outliers from numerical features. This process is essential for ensuring data quality and reliability for subsequent analysis."

###################################################
## Creating Table and Columns ##
###################################################

# Create Table:
"This code creates a copy of the 'mobile_data_2015_2017' table by selecting specific columns and saves it as 'mobile_data_2015_2017_cleaned'"

CREATE TABLE IF NOT EXISTS `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned` AS
SELECT
  date,
  hour,
  lat,
  long,
  signal,
  network,
  operator,
  status,
  description,
  net,
  speed,
  satellites,
  precission,
  provider,
  activity,
  downloadSpeed,
  uploadSpeed,
  postal_code,
  town_name,
  position_geom
FROM
  `bigquery-public-data.catalonian_mobile_coverage_eu.mobile_data_2015_2017`
;

# Add 'province' column:
ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ADD COLUMN province STRING;

# Add 'province' values:

"""This code assigns province values based on the first two digits of the 'postal_code' column.  If the first two digits match predefined values (08, 25, 17, or 43), it assigns the corresponding province name (Barcelona, Lleida, Girona, or Tarragona). If the 'postal_code' doesn't match any of these values, it assigns 'Not defined.' This update is only performed for rows where the 'province' column is currently NULL."""

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET province = CASE
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '08' THEN 'Barcelona'
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '25' THEN 'Lleida'
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '17' THEN 'Girona'
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '43' THEN 'Tarragona'
  ELSE 'Not defined'
END
WHERE province IS NULL;

# Add 'year', 'month', and 'hour_24h' columns and values:
ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ADD COLUMN year INT64;

ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ADD COLUMN month INT64;

ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ADD COLUMN hour_24h INT64;

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET province = CASE
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '08' THEN 'Barcelona'
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '25' THEN 'Lleida'
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '17' THEN 'Girona'
  WHEN LEFT(CAST(postal_code AS STRING), 2) = '43' THEN 'Tarragona'
  ELSE 'Not defined'
END
WHERE province IS NULL;

# Add 'year' , month, and 'hour' columns and values
ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ADD COLUMN year INT64;

ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ADD COLUMN month INT64;

ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ADD COLUMN hour_24h INT64;

"""This code updates the 'year' column in the 'table.' It calculates the year value by extracting the year from the 'date' column and assigns it to the 'year' column. This update is only performed for rows where the 'year' column is currently NULL."""

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET year = EXTRACT(YEAR FROM date)
WHERE year IS NULL;

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET month = EXTRACT(MONTH FROM date)
WHERE month IS NULL;

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET hour_24h = EXTRACT(HOUR FROM hour)
WHERE hour_24h IS NULL;

"""This code removes the 'date' and 'hour' columns from the 'table.' It effectively drops these columns, eliminating them from the table's schema and data."""

# Drop date and hour columns:
ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
DROP COLUMN date,
DROP COLUMN hour;

###################################################
## Categorical Features ##
###################################################

# Normalize Operator Names:

-- Original 'operator' count: 264
SELECT COUNT(DISTINCT(operator)) raw_operators 
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`;

-- Modify 'operator' names:

"""This code standardizes the 'operator' names in the 'table.' It employs various functions like UPPER (to convert to uppercase), TRIM (to remove leading/trailing spaces), and LIKE (with % wildcard for pattern matching) to recognize and group similar operator names under a common name. For example, 'VODAFONE' and 'VF ES' are both categorized as 'Vodafone.' It ensures uniformity in the 'operator' column, making the data more consistent and easier to work with."""

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET operator = CASE
    WHEN UPPER(TRIM(operator)) LIKE '%VODAFONE%' THEN 'Vodafone'
    WHEN UPPER(TRIM(operator)) = 'VF ES' THEN 'Vodafone'
    WHEN UPPER(TRIM(operator)) LIKE 'O2%' THEN 'O2'
    WHEN UPPER(TRIM(operator)) LIKE '%ORANGE%' THEN 'Orange'
    WHEN UPPER(TRIM(operator)) LIKE '%MOVISTAR%' THEN 'Movistar'
    WHEN UPPER(TRIM(operator)) LIKE '%MOBISTAR%' THEN 'Movistar'
    WHEN (TRIM(operator)) = 'Telefonica Moviles Espana' THEN 'Movistar'
    WHEN (TRIM(operator)) = 'Telef├│nica M├│viles Espa├▒a' THEN 'Movistar'
    WHEN UPPER(TRIM(operator)) LIKE 'YOIGO%' THEN 'Yoigo'
    WHEN UPPER(TRIM(operator)) LIKE 'JAZZTEL%' THEN 'Jazztel'
    WHEN UPPER(TRIM(operator)) LIKE 'MASMOVIL%' THEN 'Masmovil'
    WHEN UPPER(TRIM(operator)) LIKE 'CLARO%' THEN 'Claro'
    WHEN UPPER(TRIM(operator)) LIKE 'REPUBLICA%' THEN 'Republica Movil'
    WHEN UPPER(TRIM(operator)) LIKE 'T-MOBILE%' THEN 'T-Mobile'
    WHEN UPPER(TRIM(operator)) LIKE 'E-%' THEN 'EE'
    WHEN UPPER(TRIM(operator)) LIKE '3 %' THEN 'Three'
    WHEN UPPER(TRIM(operator)) = '3' THEN 'Three'   
    WHEN UPPER(TRIM(operator)) LIKE 'VIVO%' THEN 'Vivo' 
    WHEN LEFT(UPPER(TRIM(operator)), 3) = 'NOM' THEN 'Nomes trucades demergencia'
    WHEN LEFT(UPPER(TRIM(operator)), 8) = 'BUSCANDO' THEN 'Buscando servicio'
    WHEN LEFT(UPPER(TRIM(operator)), 14) = 'FRANCE TELECOM' THEN 'France Telcom Espana SA'
    WHEN UPPER(TRIM(operator)) = 'BITEL' THEN 'Bytel'
    WHEN UPPER(TRIM(operator)) LIKE '%BYTEL%' THEN 'Bytel'
    WHEN UPPER(TRIM(operator)) = 'CABLEMOVIL' THEN 'Cable Movil'
    WHEN UPPER(TRIM(operator)) = 'CABLE MOVIL' THEN 'Cable Movil'
    WHEN UPPER(TRIM(operator)) LIKE '%BOUYGUES%' THEN 'Bouygues Telecom'
    WHEN UPPER(TRIM(operator)) LIKE 'LOWI%' THEN 'Lowi'
    WHEN UPPER(TRIM(operator)) LIKE '%MOBILAND%' THEN 'Mobiland'
    WHEN UPPER(TRIM(operator)) LIKE 'MTS%' THEN 'MTS'
    WHEN UPPER(TRIM(operator)) LIKE 'PLAY%' THEN 'Play'
    WHEN UPPER(TRIM(operator)) = 'PEPEPHONE' THEN 'Pepephone'
    WHEN UPPER(TRIM(operator)) = 'PELEPHONE' THEN 'Pepephone'
    WHEN UPPER(TRIM(operator)) = 'PROXIMUS' THEN 'Proximus'
    WHEN UPPER(TRIM(operator)) = 'SIMYO%' THEN 'Simyo'
    WHEN UPPER(TRIM(operator)) = 'TDC' THEN 'TDC Mobil'
    WHEN UPPER(TRIM(operator)) LIKE 'TIGO%' THEN 'TIGO'
    WHEN UPPER(TRIM(operator)) LIKE 'TIM%' THEN 'TIM'
    WHEN UPPER(TRIM(operator)) LIKE 'TELEKOM %' THEN 'Telekom'
    WHEN UPPER(TRIM(operator)) LIKE 'TELENOR%' THEN 'Telenor'
    WHEN UPPER(TRIM(operator)) LIKE 'VODACOM%' THEN 'Vodacom'
    WHEN UPPER(TRIM(operator)) LIKE 'CUBACEL%' THEN 'Cubacel'
    WHEN UPPER(TRIM(operator)) LIKE 'MOVILNET%' THEN 'Movilnet'
    WHEN UPPER(TRIM(operator)) = 'NULL' THEN NULL
    ELSE operator
END
WHERE operator IS NOT NULL; -- Final'operator' count: 142


# Normalize Network Names:

-- Original 'network' count: 230
SELECT COUNT(DISTINCT(network)) raw_network
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`;

-- Update network names
UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET network = CASE
    WHEN UPPER(TRIM(network)) LIKE '%VODAFONE%' THEN 'Vodafone'
    WHEN UPPER(TRIM(network)) = 'VF ES' THEN 'Vodafone'
    WHEN UPPER(TRIM(network)) LIKE 'O2%' THEN 'O2'
    WHEN UPPER(TRIM(network)) LIKE '%ORANGE%' THEN 'Orange'
    WHEN UPPER(TRIM(network)) LIKE '%MOVISTAR%' THEN 'Movistar'
    WHEN UPPER(TRIM(network)) LIKE '%MOBISTAR%' THEN 'Movistar'
    WHEN (TRIM(network)) = 'Telefonica Moviles Espana' THEN 'Movistar'
    WHEN (TRIM(network)) = 'Telef├│nica M├│viles Espa├▒a' THEN 'Movistar'
    WHEN UPPER(TRIM(network)) LIKE 'YOIGO%' THEN 'Yoigo'
    WHEN UPPER(TRIM(network)) LIKE 'JAZZTEL%' THEN 'Jazztel'
    WHEN UPPER(TRIM(network)) LIKE 'MASMOVIL%' THEN 'Masmovil'
    WHEN UPPER(TRIM(network)) LIKE 'CLARO%' THEN 'Claro'
    WHEN UPPER(TRIM(network)) LIKE 'REPUBLICA%' THEN 'Republica Movil'
    WHEN UPPER(TRIM(network)) LIKE 'T-MOBILE%' THEN 'T-Mobile'
    WHEN UPPER(TRIM(network)) LIKE 'E-%' THEN 'EE'
    WHEN UPPER(TRIM(network)) LIKE '3 %' THEN 'Three'
    WHEN UPPER(TRIM(network)) = '3' THEN 'Three'   
    WHEN UPPER(TRIM(network)) LIKE 'VIVO%' THEN 'Vivo'
    WHEN LEFT(UPPER(TRIM(network)), 14) = 'FRANCE TELECOM' THEN 'France Telcom Espana SA'
    WHEN UPPER(TRIM(network)) = 'BITEL' THEN 'Bytel'
    WHEN UPPER(TRIM(network)) LIKE '%BYTEL%' THEN 'Bytel'
    WHEN UPPER(TRIM(network)) = 'CABLEMOVIL' THEN 'Cable Movil'
    WHEN UPPER(TRIM(network)) = 'CABLE MOVIL' THEN 'Cable Movil'
    WHEN UPPER(TRIM(network)) LIKE '%BOUYGUES%' THEN 'Bouygues Telecom'
    WHEN UPPER(TRIM(network)) LIKE 'LOWI%' THEN 'Lowi'
    WHEN UPPER(TRIM(network)) LIKE '%MOBILAND%' THEN 'Mobiland'
    WHEN UPPER(TRIM(network)) LIKE 'MTS%' THEN 'MTS'
    WHEN UPPER(TRIM(network)) LIKE 'PLAY%' THEN 'Play'
    WHEN UPPER(TRIM(network)) = 'PEPEPHONE' THEN 'Pepephone'
    WHEN UPPER(TRIM(network)) = 'PELEPHONE' THEN 'Pepephone'
    WHEN UPPER(TRIM(network)) = 'PROXIMUS' THEN 'Proximus'
    WHEN UPPER(TRIM(network)) = 'SIMYO%' THEN 'Simyo'
    WHEN UPPER(TRIM(network)) = 'TDC' THEN 'TDC Mobil'
    WHEN UPPER(TRIM(network)) LIKE 'TIGO%' THEN 'TIGO'
    WHEN UPPER(TRIM(network)) LIKE 'TIM%' THEN 'TIM'
    WHEN UPPER(TRIM(network)) LIKE 'TELEKOM %' THEN 'Telekom'
    WHEN UPPER(TRIM(network)) LIKE 'TELENOR%' THEN 'Telenor'
    WHEN UPPER(TRIM(network)) LIKE 'VODACOM%' THEN 'Vodacom'
    WHEN UPPER(TRIM(network)) LIKE 'CUBACEL%' THEN 'Cubacel'
    WHEN UPPER(TRIM(network)) LIKE 'MOVILNET%' THEN 'Movilnet'
    ELSE network
END
WHERE network IS NOT NULL; -- Final 'network' count: 136

# Normalize Postal Code and Town Names:
-- This part ensures that there are no discrepancies between postal codes and town names, and removes rows with missing values in both columns.

  -- Count rows where the postal code is NULL while the town name is not NULL: 0.
SELECT postal_code, town_name
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
WHERE postal_code IS NULL
AND town_name IS NOT NULL;

  -- Count rows where the postal code is not NULL while the town name is NULL: 0.
SELECT postal_code, town_name
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
WHERE postal_code IS NOT NULL
AND town_name IS NULL;

  -- Total count where postal_code and town_name are both NULL: 926181 rows
SELECT COUNT (*) no_cp_town
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
WHERE postal_code IS NULL AND town_name IS NULL;

-- Drop rows with Null postal_code and town_name values
DELETE FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
WHERE postal_code IS NULL AND town_name IS NULL;

# Normalize 'net' Column:

SELECT DISTINCT(net)
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`;

-- NULL count: 714511
SELECT COUNT(*)
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
WHERE net IS NULL;

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET net = CASE
  WHEN net IS NULL THEN 'Undefined net'
  ELSE net
END
WHERE net IS NULL;

# Download and Upload Speed Columns:

SELECT downloadSpeed FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned` WHERE downloadSpeed IS NOT NULL limit 100; -- record count: 0

SELECT uploadSpeed FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned` WHERE uploadSpeed IS NOT NULL limit 100;     -- record count: 0

-- Drop both columns
ALTER TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
DROP COLUMN downloadSpeed,
DROP COLUMN uploadSpeed;

# Normalize Provider Names:

-- Original 'provider' count: 13
SELECT provider, COUNT(*) record_count
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
GROUP BY 1;

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET provider = CASE
    WHEN UPPER(TRIM(provider)) LIKE '%GPS%' THEN 'GPS'
    WHEN UPPER(TRIM(provider)) LIKE '%FUSED%' THEN 'Fused'
    WHEN UPPER(TRIM(provider)) LIKE '%NETWORK%' THEN 'Network'
    ELSE provider
END
WHERE provider IS NOT NULL; 

-- Drop null values and wrong providers

"""Remove rows with specified provider values and NULL providers from the table."""

DELETE FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
WHERE provider IN (
  '4','22','19','2017-081503867352116,', 'local_database', 'disabled'
) OR provider IS NULL; -- Final 'provider' count: 3

# Normalize 'description' and 'activity':

-- Description
SELECT DISTINCT(description)
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`;

-- Activity
SELECT DISTINCT(activity)
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`;

"""Assign 'UNKNOWN' to rows with NULL activity values in the table."""

UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
SET activity = CASE 
  WHEN activity IS NULL THEN 'UNKNOWN' 
END
WHERE activity IS NULL;

###################################################
## Numerical Features ##
###################################################

# Calculate Summary Statistics:

""" This piece of code calculates summary statistics for various numerical features such as 'status', 'speed', 'satellites', 'precission', and 'signal'."""

SELECT
  'status' AS metric,
  MIN(status) AS min_value,
  MAX(status) AS max_value,
  ROUND(AVG(status), 2) AS avg_value,
  ROUND(STDDEV_POP(status), 2) AS std_value
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
UNION ALL
SELECT
  'speed' AS metric,
  MIN(speed) AS min_value,
  MAX(speed) AS max_value,
  ROUND(AVG(speed), 2) AS avg_value,
  ROUND(STDDEV_POP(speed), 2) AS std_value
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
UNION ALL
SELECT
  'satellites' AS metric,
  MIN(satellites) AS min_value,
  MAX(satellites) AS max_value,
  ROUND(AVG(satellites), 2) AS avg_value,
  ROUND(STDDEV_POP(satellites), 2) AS std_value
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
UNION ALL
SELECT
  'precision' AS metric, -- max 5304, avg 23.11; std 25
  MIN(precission) AS min_value,
  MAX(precission) AS max_value,
  ROUND(AVG(precission), 2) AS avg_value,
  ROUND(STDDEV_POP(precission), 2) AS std_value
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
UNION ALL
SELECT
  'signal' AS metric,
  MIN(signal) AS min_value,
  MAX(signal) AS max_value,
  ROUND(AVG(signal), 2) AS avg_value,
  ROUND(STDDEV_POP(signal), 2) AS std_value
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`;

# Handling 'precission' Possible Outliers:

SELECT precission
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
ORDER BY 1 DESC;

"""Calculate various percentiles (25th, 50th, 75th, 90th, 95th, 99th) for the 'precission' column in the table and display their values."""

WITH Percentiles AS (
  SELECT
    APPROX_QUANTILES(precission, 100)[OFFSET(25)] AS percentile_25,
    APPROX_QUANTILES(precission, 100)[OFFSET(50)] AS median,
    APPROX_QUANTILES(precission, 100)[OFFSET(75)] AS percentile_75,
    APPROX_QUANTILES(precission, 100)[OFFSET(90)] AS percentile_90,
    APPROX_QUANTILES(precission, 100)[OFFSET(95)] AS percentile_95,
    APPROX_QUANTILES(precission, 100)[OFFSET(99)] AS percentile_99
  FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
)
SELECT percentile_25, median, percentile_75, percentile_90, percentile_95, percentile_99
FROM Percentiles; -- Percentile 99: top 1% of values (around 100k rows) are greater than a precision of 120.

-- How would the removal of outliers affect the data distribution, and would this impact be consistent across different provinces?

"""This query calculates and compares outlier percentages in different provinces based on the 'precission' column, where values above 120 are considered outliers. It provides insights into how outliers are distributed among provinces."""

SELECT
  province,
  COUNT(*) AS province_records,
  SUM(CASE WHEN precission > 120 THEN 1 ELSE 0 END) AS province_outliers,
  ROUND((SUM(CASE WHEN precission > 120 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS percentage_outliers
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
GROUP BY 1
ORDER BY 1; -- % outliers (Bcn, Girona, Tarragona: 1.06% / Lleida: 0.45%)

-- Outliers in the 'precission' column, identified as values exceeding 120, are now removed from the dataset.
DELETE FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`
WHERE precission > 120;

-- New 'precission' summary statistics: max 120, avg 21.93, std 17.76

###################################################
## Save Cleaned Table ##
###################################################

# Save Cleaned Table:
-- The final cleaned dataset is saved as 'mobile_data_2015_2017_cleaned_final' for further analysis and reporting.
CREATE OR REPLACE TABLE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned_final`
AS
SELECT *
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.mobile_data_2015_2017_cleaned`;

