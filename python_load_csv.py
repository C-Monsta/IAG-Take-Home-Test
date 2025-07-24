import duckdb

# Connect to (or create) a DuckDB database file
con = duckdb.connect("IAG.db")

# Load the CSV file into a table
con.execute(r"""
    CREATE OR REPLACE TABLE policy_event_data AS
    SELECT * FROM read_csv_auto('C:\Users\kyles\OneDrive\Desktop\IAG\policy_events.csv')
""")

# Print a preview
df = con.execute("SELECT * FROM policy_event_data LIMIT 5").fetchdf()
print(df)
