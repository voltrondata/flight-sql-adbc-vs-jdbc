# Be sure to run script: "create_local_duckdb_database.py" first
import duckdb
from utils import Timer, TIMER_TEXT, BENCHMARK_SQL_STATEMENT, DUCKDB_DB_FILE


# Gets Database Connection
with Timer(name=f"\nLocal DuckDB - Fetch data from lineitem table", text=TIMER_TEXT):
    with duckdb.connect(database=DUCKDB_DB_FILE.as_posix()) as conn:
        pyarrow_table = conn.execute(query=BENCHMARK_SQL_STATEMENT).fetch_arrow_table()
        print(f"Number of rows fetched: {pyarrow_table.num_rows}")
