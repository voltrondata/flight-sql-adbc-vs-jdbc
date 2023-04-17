import adbc_driver_flightsql.dbapi as flight_sql
from utils import Timer, TIMER_TEXT, NUMBER_OF_RUNS, BENCHMARK_SQL_STATEMENT, FlightDatabaseConnection, FLIGHT_DB


def benchmark_adbc(db: FlightDatabaseConnection = FLIGHT_DB,
                   query: str = BENCHMARK_SQL_STATEMENT
                   ):
    with Timer(name=f"\nADBC - Fetch data from lineitem table", text=TIMER_TEXT):
        with flight_sql.connect(uri=f"grpc+tls://{db.hostname}:{str(db.port)}",
                                db_kwargs={"username": db.username,
                                           "password": db.password,
                                           "adbc.flight.sql.client_option.tls_skip_verify": str(db.disableCertificateVerification).lower()
                                           }
                                ) as conn:
            with conn.cursor() as cur:
                cur.execute(operation=query)
                pyarrow_table = cur.fetch_arrow_table()
                print(f"Number of rows fetched: {pyarrow_table.num_rows}")


if __name__ == "__main__":
    import timeit

    total_time = timeit.timeit(stmt="benchmark_adbc()",
                               setup="from __main__ import benchmark_adbc",
                               number=NUMBER_OF_RUNS
                               )

    print((f"Number of runs: {NUMBER_OF_RUNS}\n"
           f"Total time: {total_time}\n"
           f"Average time: {total_time / float(NUMBER_OF_RUNS)}"
           )
          )
