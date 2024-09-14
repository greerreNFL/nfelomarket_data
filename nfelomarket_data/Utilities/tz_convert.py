import pandas as pd

def tz_convert(
    df:pd.DataFrame,
    tz:str='America/Los_Angeles'
):
    """
    Converts all datetime columns in the DataFrame to single timezone
    to ensure no errors in joins, merges, combines, etc.
    
    Parameters:
    - df: pandas DataFrame containing datetime columns.
    - tz: time zone to convert to
    
    Returns:
    - pandas DataFrame with all datetime columns converted to a tz.
    """
    
    def convert_datetime_to_pst(column):
        return (
            pd.to_datetime(column)  # Convert to datetime
            .dt.tz_convert('UTC')  # Convert to UTC first
            .dt.tz_convert(tz)  # Convert to tz
        )
    
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = convert_datetime_to_pst(df[column])
    
    return df