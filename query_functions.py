"""
query_functions.py

This Python module provides two main functions for executing SQL queries with a pre-configured Google BigQuery client object. These functions are useful for working with Google BigQuery, storing query results in a Pandas DataFrame, or directly updating tables. To use these functions, follow the import instructions below and examples of their usage.

Functions:
1. query_df(query)
    - Purpose:
      - Execute SQL queries using a pre-configured Google BigQuery client object.
      - Store the results in a Pandas DataFrame.
    - Arguments:
      - query (str): The SQL query to execute.
    - Example Usage:
      - Call the function with the query as an argument.
        Example: query_df(query)
  
2. run_query(query)
    - Purpose:
      - Execute SQL queries using a pre-configured Google BigQuery client object.
      - Directly update a table, suitable for queries like UPDATE or DELETE.
    - Arguments:
      - query (str): The SQL query to execute.
    - Example Usage:
      - Call the function with the query as an argument.
        Example: run_query(query)
        
To use these functions, follow these steps:

1. Import the functions in your Python script or Jupyter Notebook.
   Example:
    # from query_functions import query_df
    # from query_functions import run_query

2. Make sure to configure your BigQuery client by providing the path to your BigQuery key file. This path should be stored in a file named "bq_key_path.txt."

3. Call the imported functions with your SQL queries to interact with your BigQuery datasets and tables.

Remember to adjust the "bq_key_path.txt" file with the appropriate path to your BigQuery key file.

This module simplifies your interaction with Google BigQuery, making it easier to work with SQL queries and data in a BigQuery environment.


Functions:
"""

def query_df(query):
    """
    This function allows the user to execute SQL queries using a pre-configured BigQuery client
    object, store the results in a Pandas DataFrame, and specify the path to a BigQuery key file.

    Args:
    - query (str): The SQL query to execute.

    Example Usage:
    1. Call the function with the query as an argument.

    Example:
    1. Call query_df(query) to execute SQL queries.
    """
    
    # Import required libraries
    import pandas as pd
    from google.cloud import bigquery
    
    """ Create a BigQuery client """
    
    # Read the path to your BigQuery key file
    with open("bq_key_path.txt", "r") as f:
        credentials_path = f.read()

    # Remove any newline characters from the path
    credentials_path = credentials_path.strip()

    # Create a BigQuery client using the credentials
    client = bigquery.Client.from_service_account_json(credentials_path)
    
    """ Execute the query """
    
    # Execute the query using the pre-configured client object
    query_job = client.query(query)

    # Get the query result
    results = query_job.result()

    # Initialize an empty list to store rows
    rows = []

    # Get the schema to retrieve column names
    schema = results.schema

    # Extract column names from the schema
    column_names = [field.name for field in schema]

    # Iterating through the results
    for row in results:
        # Append each row as a list to the rows list
        rows.append(list(row))

    """ Store results in a DataFrame"""    
    
    # Create a DataFrame from the list of rows with column names
    df = pd.DataFrame(rows, columns=column_names)
    
    # Print the DataFrame
    #display(df)
    
    # Return the DataFrame
    return df


# Same but without printing nor storing the result in a dataset. Useful for queries like UPDATE or DELETE

def run_query(query):
    """
    This function allows the user to execute SQL queries using a pre-configured BigQuery client
    object and directly update the table.

    Args:
    - query (str): The SQL query to execute.

    Example Usage:
    1. Call the function with the query as an argument.

    Example:
    1. run_query_and_update(query) to execute SQL queries and update the table.
    """
    
    # Import required libraries
    from google.cloud import bigquery
    
    """ Create a BigQuery client """
    
    # Read the path to your BigQuery key file
    with open("bq_key_path.txt", "r") as f:
        credentials_path = f.read()

    # Remove any newline characters from the path
    credentials_path = credentials_path.strip()

    # Create a BigQuery client using the credentials
    client = bigquery.Client.from_service_account_json(credentials_path)
    
    """ Execute the query and update the table """
    
    # Execute the query using the pre-configured client object
    try:
        query_job = client.query(query)
        # Wait for the query to finish (update to complete)
        query_job.result()
        print("Query successfully executed, and the table has been updated.")
    except Exception as e:
        # Query execution encountered an error
        print(f"Error executing query: {str(e)}")


