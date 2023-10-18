###################################################
## SQL Script for Creating Project Tables ##
###################################################

#BigQuery query link

    cat_pop_by_province: """https://console.cloud.google.com/bigquery?sq=779325856445:14ba69fe1656439aaa54f04f7e58c51a"""

    cat_percapita_income_by_province_2015_2017: """https://console.cloud.google.com/bigquery?sq=779325856445:2c27abc25c904eafad2cfdd84d49fd1e"""

# Purpose:

"This SQL code is designed to create two project tables in Google BigQuery to support data analysis and reporting. The tables 'cat_pop_by_province_2015_2017' and 'cat_percapita_income_by_province_2015_2017' are created from scratch and populated with data related to population, area, density, and per capita income by province for the years 2015, 2016, and 2017. This data is crucial for various project tasks and analyses."



###################################################
## Create the cat_pop_by_province_2015_2017 Table ##
###################################################

-- Create the table if it doesn't exist with 'square_km' as INT64
CREATE TABLE IF NOT EXISTS `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_pop_by_province_2015_2017` (
  `province` STRING,
  `year` INT64,
  `population` INT64,
  `sq_km` INT64,
  `density_per_sq_km` FLOAT64
);

-- Insert values into the table
INSERT INTO `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_pop_by_province_2015_2017` (
  `province`,
  `year`,
  `population`
) VALUES
  ('Barcelona', 2015, 5618162),
  ('Barcelona', 2016, 5635085),
  ('Barcelona', 2017, 5652301),
  ('Lleida', 2015, 742138),
  ('Lleida', 2016, 742099),
  ('Lleida', 2017, 741884),
  ('Girona', 2015, 765783),
  ('Girona', 2016, 766273),
  ('Girona', 2017, 766705),
  ('Tarragona', 2015, 811089),
  ('Tarragona', 2016, 810947),
  ('Tarragona', 2017, 810600);

-- Update sq_km
UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_pop_by_province_2015_2017`
SET
  `sq_km` = CASE
    WHEN province = 'Barcelona' THEN 7726
    WHEN province = 'Lleida' THEN 12172
    WHEN province = 'Girona' THEN 5908
    WHEN province = 'Tarragona' THEN 6303
  END
WHERE sq_km IS NULL;

-- Update density_per_sq_km
UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_pop_by_province_2015_2017`
SET density_per_sq_km = ROUND(population / sq_km,1)
WHERE density_per_sq_km IS NULL;

-- Final table
SELECT * FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_pop_by_province_2015_2017`;



#################################################################
## Create the cat_percapita_income_by_province_2015_2017 Table ##
#################################################################

-- Create the table cat_percapita_income_by_province_2015_2017 if it doesn't exist
CREATE TABLE IF NOT EXISTS `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_percapita_income_by_province_2015_2017` (
  `year` INT64,
  `province` STRING,
  `per_capita_income` INT64
);

-- Insert values into the table
INSERT INTO `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_percapita_income_by_province_2015_2017` (
  `year`,
  `province`,
  `per_capita_income`
) VALUES
  (2015, 'Barcelona', 27214),
  (2016, 'Barcelona', 27913),
  (2017, 'Barcelona', 28481),
  (2015, 'Tarragona', 22486),
  (2016, 'Tarragona', 23130),
  (2017, 'Tarragona', 23534),
  (2015, 'Lleida', 20136),
  (2016, 'Lleida', 20713),
  (2017, 'Lleida', 23534),
  (2015, 'Girona', 25200),
  (2016, 'Girona', 25598),
  (2017, 'Girona', 25992);

-- Fix value
UPDATE `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_percapita_income_by_province_2015_2017`
SET per_capita_income = 21091
WHERE year = 2017 AND province = 'Lleida';

-- Final table
SELECT *
FROM `bq-analyst-230590.project_cat_mobile_coverage_2015_2017.cat_percapita_income_by_province_2015_2017`
ORDER BY per_capita_income DESC;

