import duckdb
from utils import DUCKDB_DB_FILE

# Delete the database if it exists...
DUCKDB_DB_FILE.unlink(missing_ok=True)

# Get a DuckDB database connection
with duckdb.connect(database=DUCKDB_DB_FILE.as_posix()) as conn:
    # Install the TPCH extension needed to generate the data...
    conn.install_extension(extension="tpch")
    conn.load_extension(extension="tpch")

    # Generate the data
    conn.execute(query=f"CALL dbgen(sf=1)")

    print(f"Created DuckDB Database file: {DUCKDB_DB_FILE.as_posix()}")
