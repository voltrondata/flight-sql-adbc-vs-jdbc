import os
from dotenv import load_dotenv
import adbc_driver_flightsql.dbapi as flight_sql
from utils import Timer, TIMER_TEXT, BENCHMARK_SQL_STATEMENT

with Timer(name=f"\nADBC - Fetch data from lineitem table", text=TIMER_TEXT):
    # Load our environment for the password...
    load_dotenv(dotenv_path=".env")

    flight_password = os.environ["FLIGHT_PASSWORD"]

    with flight_sql.connect(uri="grpc+tls://localhost:31337",
                            db_kwargs={"username": "flight_username",
                                       "password": flight_password,
                                       "adbc.flight.sql.client_option.tls_skip_verify": "true"
                                       }
                            ) as conn:
        with conn.cursor() as cur:
            cur.execute(operation=BENCHMARK_SQL_STATEMENT)
            pyarrow_table = cur.fetch_arrow_table()
            print(f"Number of rows fetched: {pyarrow_table.num_rows}")
