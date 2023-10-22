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


## OLD VERSION
#def query_df(query, df_name):
#    """
#    This function allows the user to execute SQL queries using a pre-configured BigQuery client
#    object, store the results in a Pandas DataFrame with a user-defined name, and specify the path
#    to a BigQuery key file.
#    
#    Args:
#    - query (str): The SQL query to execute.
#    - df_name (str): The name for the resulting DataFrame.

#    Example Usage:
#    1. Call the function with the query and df_name as arguments.

#    Example:
#    1. Call query_df(query, df_name) to execute SQL queries.
#    """
    
#    # Import required libraries
#    import pandas as pd
#    from google.cloud import bigquery
#    from IPython.display import display
#    from IPython.display import Markdown
    
#    """ Create a BigQuery client """
    
#    # Read the path to your new BigQuery key file
#    with open("bq_key_path.txt", "r") as f:
#        credentials_path = f.read()

#    # Remove any newline characters from the path
#    credentials_path = credentials_path.strip()

#    # Create a BigQuery client using the credentials
#    client = bigquery.Client.from_service_account_json(credentials_path)
    
#    """ Execute the query """
    
#    # Execute the query using the pre-configured client object
#    query_job = client.query(query)

#    # Get the query result
#    results = query_job.result()

    # Initialize an empty list to store rows
#    rows = []

#    # Get the schema to retrieve column names
#    schema = results.schema

#    # Extract column names from the schema
#    column_names = [field.name for field in schema]

#    # Iterating through the results
#    for row in results:
#        # Append each row as a list to the rows list
#        rows.append(list(row))

#    """ Store results in a DataFrame"""    
    
#    # Create a DataFrame from the list of rows with column names
#    df = pd.DataFrame(rows, columns=column_names)

#    # Set the DataFrame as a variable with the user-defined name using globals()
#    globals()[df_name] = df
    
#    # Print SQL Query
#    display(Markdown(f"Query: "))
#    print(f"{query}")
    
#    # Print DataFrame name
#    display(Markdown(f"Dataframe: **{df_name}**"))    
    
#    # Return the DataFrame with the new assigned name
#    return df



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


