# Be sure to run script: "create_local_duckdb_database.py" first
import duckdb
from utils import Timer, TIMER_TEXT, DUCKDB_DB_FILE, NUMBER_OF_RUNS, BENCHMARK_SQL_STATEMENT


def benchmark_duckdb(query: str = BENCHMARK_SQL_STATEMENT):
    with Timer(name=f"\nLocal DuckDB - Fetch data from lineitem table",
               text=TIMER_TEXT,
               initial_text=True
               ):
        with duckdb.connect(database=DUCKDB_DB_FILE.as_posix()) as conn:
            pyarrow_table = conn.execute(query=query).fetch_arrow_table()
            print(f"Number of rows fetched: {pyarrow_table.num_rows}")


if __name__ == "__main__":
    import timeit

    total_time = timeit.timeit(stmt="benchmark_duckdb()",
                               setup="from __main__ import benchmark_duckdb",
                               number=NUMBER_OF_RUNS
                               )

    print((f"Number of runs: {NUMBER_OF_RUNS}\n"
           f"Total time: {total_time}\n"
           f"Average time: {total_time / float(NUMBER_OF_RUNS)}"
           )
          )
