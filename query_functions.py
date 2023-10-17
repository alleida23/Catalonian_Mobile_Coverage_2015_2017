def query_df(query, df_name):
    """
    This function allows the user to execute SQL queries using a pre-configured BigQuery client
    object, store the results in a Pandas DataFrame with a user-defined name, and specify the path
    to a BigQuery key file.

    Args:
    - query (str): The SQL query to execute.
    - df_name (str): The name for the resulting DataFrame.

    Example Usage:
    1. Call the function with the query and df_name as arguments.

    Example:
    1. Call query_df(query, df_name) to execute SQL queries.
    """
    
    # Import required libraries
    import pandas as pd
    from google.cloud import bigquery
    from IPython.display import display
    from IPython.display import Markdown
    
    """ Create a BigQuery client """
    
    # Read the path to your new BigQuery key file
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

    # Set the DataFrame as a variable with the user-defined name using globals()
    globals()[df_name] = df
    
    # Print SQL Query
    display(Markdown(f"Query: "))
    print(f"{query}")
    
    # Print DataFrame name
    display(Markdown(f"Dataframe: **{df_name}**"))    
    
    # Return the DataFrame with the new assigned name
    return df
