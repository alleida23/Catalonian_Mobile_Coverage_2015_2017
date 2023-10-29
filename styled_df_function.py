"""
The 'styled_df_by_value' function enables conditional formatting of specific rows in a Pandas DataFrame based on the presence of defined values within a specified column. This function's primary objective is to highlight and visually distinguish certain rows in a table, making it convenient for quick identification of key information or specific results within the dataset.

By utilizing color-coded styling, this function allows users to specify certain values or groups of values within a column and apply distinct background colors to the entire rows in which these values are found. This is particularly useful when needing to emphasize or visually segregate rows that contain specific key data, thereby enhancing the readability and analysis of the tabular information.
"""

def styled_df_by_value(df, column, *value_groups):
    """
    Conditionally styles DataFrame rows based on specified values in a given column.

    Parameters:
    - df: pandas.DataFrame
        The DataFrame to be styled.
    - column: str
        The column in the DataFrame to be checked for values.
    - *value_groups: tuple(s) of lists
        One or more tuples containing lists of values to be highlighted in the DataFrame.

    Returns:
    - pandas.io.formats.style.Styler
        A Styler object with row styles applied based on the specified values and their associated colors.
    """

    import pandas as pd

    # Define a palette of purple colors
    purple_palette = ['#9370DB', '#8A2BE2', '#800080', '#4B0082', '#483D8B', '#00008B', '#0000CD']

    # Create a dictionary to map values to colors for each tuple in value_groups
    styles_dict = {}
    color_index = 0
    
    for group in value_groups:
        color = purple_palette[color_index]

        for value in group:
            styles_dict[value] = color

        color_index += 1

    def highlight_by_value(row):
        # Retrieve colors based on the values in the row's specified column
        colors = [styles_dict.get(row[column], "") for _ in row]
        return [f'background-color: {color}' for color in colors]

    # Apply the row highlighting based on the values and their associated colors
    return df.style.apply(highlight_by_value, axis=1)
