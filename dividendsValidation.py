import pandas as pd
from sqlalchemy import create_engine

# Define the PostgreSQL database connection string in the correct format
conn_string = (
    "use yours"
)

# Create the connection engine
engine = create_engine(conn_string)

# Function to load proventos data from a table
def load_proventos_data(table):
    query = f"SELECT * FROM {table}"
    return pd.read_sql(query, engine)

# Load data from the assets.dividend and bdrs.dividend tables
df_assets = load_proventos_data("assets.dividend")
df_bdrs = load_proventos_data("bdrs.dividend")

# Combine the two DataFrames to validate together
df = pd.concat([df_assets, df_bdrs])

# Schema Validation
expected_schema = {
    'dividendid': 'int64',
    'assetid': 'int64',
    'rank': 'int32',
    'rankpayment': 'Int32',
    'datecom': 'datetime64[ns]',
    'datepayment': 'datetime64[ns]',
    'value': 'float64',
    'type': 'int16',
    'referenceprice': 'float64',
    'createdate': 'datetime64[ns]',
    'updatedate': 'datetime64[ns]',
    'isenabled': 'bool',
    'locked': 'bool',
    'netvalue': 'float64',
    'total': 'float64',
    'rankanuncio': 'Int32',
    'dateanuncio': 'datetime64[ns]',
    'originalvalue': 'float64',
    'yearreference': 'int16',
    'irrfpercent': 'float64'
}

# Handling null values before conversion
for column, dtype in expected_schema.items():
    if column in df.columns:
        if 'int' in dtype and df[column].isnull().any():
            df[column].fillna(0, inplace=True)  # Replace NaN with 0 for integers
        if 'float' in dtype and df[column].isnull().any():
            df[column].fillna(0.0, inplace=True)  # Replace NaN with 0.0 for floats

# Converting column types to expected types
for column, dtype in expected_schema.items():
    if column in df.columns:
        try:
            if 'datetime' in dtype:
                df[column] = pd.to_datetime(df[column], errors='coerce')
                if df[column].dt.tz:
                    df[column] = df[column].dt.tz_convert(None)
            else:
                df[column] = df[column].astype(dtype)
        except ValueError:
            raise ValueError(f"Error converting column {column} to type {dtype}")

# Identifying rows with null values in mandatory fields
mandatory_fields = ['datepayment', 'value']
invalid_rows = df[df[mandatory_fields].isnull().any(axis=1)]

if not invalid_rows.empty:
    print("Rows with null values in mandatory fields:")
    print(invalid_rows)
    # Optional: Remove invalid rows
    df = df.dropna(subset=mandatory_fields)
    print("Invalid rows have been removed.")

# Schema Validation
for column, dtype in expected_schema.items():
    if column not in df.columns:
        raise ValueError(f"Column {column} is missing in the data")
    if df[column].dtype != dtype and not (df[column].dtype == 'datetime64[ns, UTC]' and dtype == 'datetime64[ns]'):
        raise TypeError(f"Column {column} has incorrect type. Expected {dtype}, but found {df[column].dtype}")

# Business Rules
#if (df['datepayment'] < pd.Timestamp.today()).any():
 #   raise ValueError("There are payment dates in the past")

if (df['value'] <= 0).any():
    raise ValueError("There are negative or zero provento values")

# Checking for Duplicates
duplicates = df[df.duplicated(['assetid', 'datepayment'], keep=False)]
if not duplicates.empty:
    raise ValueError("There are duplicate records in the data")

print("All tests were successful!")
