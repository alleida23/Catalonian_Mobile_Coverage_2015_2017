# Catalonian Mobile Coverage Data Project

## Data Cleaning, Transformation, and SQL Practice with Jupyter Notebook and BigQuery

**Project by Albert Lleida**

**October 2023**

---

**Project Overview:**

This project focuses on creating a comprehensive dataset using data from the public Google BigQuery dataset "[mobile_data_2015_2017](https://console.cloud.google.com/bigquery?project=bq-analyst-230590&p=bq-analyst-230590&d=project_cat_mobile_coverage_2015_2017&t=mobile_data_2015_2017_cleaned)." Utilizing a BigQuery connection, the project involves cleaning and transforming the dataset through SQL queries, alongside practicing SQL commands and enhancing data analysis skills.

Key project components include:

- **Data Cleaning:** Thorough cleaning operations to ensure data integrity for analysis.
- **Table Creation:** Generating new tables for improved data structure and querying practices.
- **SQL Practice:** Hands-on experience with SQL commands for data manipulation and exploration.

---

**Links to Created Datasets:**

- **Main Dataset:** [Mobile Data Coverage 2015-2017](https://console.cloud.google.com/bigquery?project=bq-analyst-230590&p=bq-analyst-230590&d=project_cat_mobile_coverage_2015_2017&t=mobile_data_2015_2017_cleaned)
- **Additional Table 1:** [Catalan Population by Province 2015-2017](https://console.cloud.google.com/bigquery?project=bq-analyst-230590&p=bq-analyst-230590&d=project_cat_mobile_coverage_2015_2017&t=cat_pop_by_province_2015_2017)
- **Additional Table 2:** [Catalan Per Capita Income by Province 2015-2017](https://console.cloud.google.com/bigquery?project=bq-analyst-230590&p=bq-analyst-230590&d=project_cat_mobile_coverage_2015_2017&t=cat_percapita_income_by_province_2015_2017)

---

**Project Files and Structure:** 

1. **SQL Data Cleaning for Mobile Coverage Dataset.ipynb**  
   - Notebook focusing on cleaning and preparing a public mobile coverage dataset in BigQuery. Emphasis on data quality through normalization, handling missing data, and removing outliers.

2. **SQL Script for Creating Additional Project Tables.ipynb**  
   - Script to create essential project tables 'cat_pop_by_province_2015_2017' and 'cat_percapita_income_by_province_2015_2017' in BigQuery. Tables include data on population, area, density, and per capita income by province.

3. **Mastering SQL Queries with BigQuery - Data Exploration and Analysis.ipynb**  
   - Comprehensive kernel for data exploration and analysis using BigQuery. Focuses on mastering SQL queries for insights from the "Catalonian Mobile Coverage" dataset.

4. **query_functions.py**  
   - Python module providing functions for executing SQL queries with a pre-configured Google BigQuery client object. Useful for working with BigQuery, storing query results in Pandas DataFrames, or updating tables.

5. **styled_df_function.py**  
   - Python module offering the 'styled_df_by_value' function, enabling conditional formatting of specific rows in a Pandas DataFrame based on defined values within a specified column.

6. **BQ_Access (Folder)**  
   - Contains the JSON file necessary for BigQuery Connection. For security reasons, this file is listed in the project's `.gitignore` file to avoid inadvertent exposure or inclusion in version control.

---

**Prerequisites:**

To work on this project and access the datasets, you will need:

- A Google BigQuery account and connection.
- Jupyter Notebook installed on your system.
- SQL knowledge for working with SQL queries.

---

**Libraries and Modules Used:**

- `pandas` as `pd`
- `google.cloud.bigquery`
- `IPython.display`
- `matplotlib.pyplot` 
- `seaborn` 
- `calendar` 
- `query_functions` (customized function)
- `styled_df_function` (customized function)

---

**Acknowledgments:**

This project is made possible through the resources and support provided by Google BigQuery and the open-source data community.

---

**License:**

This project is licensed under the MIT License - see the [LICENSE](link) file for details.

---

Thank you for your interest in the Catalonian Mobile Coverage Data Project!

---

*(C) 2023, Albert Lleida*

---

Feel free to use this updated README for your project documentation.
