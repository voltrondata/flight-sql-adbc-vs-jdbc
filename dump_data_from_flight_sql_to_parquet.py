import os
import adbc_driver_flightsql.dbapi as flight_sql
import pyarrow.parquet as pq
import pyarrow
import decimal
from dotenv import load_dotenv


# Load our environment file
load_dotenv(dotenv_path=".env")


def main():
    with flight_sql.connect(uri=f"grpc+tls://flight-sql.vdfieldeng.com:31337",
                            db_kwargs={"username": "flight_username",
                                       "password": os.environ["FLIGHT_PASSWORD"],
                                       "adbc.flight.sql.client_option.tls_skip_verify": "true"
                                       }
                            ) as conn:
        with conn.cursor() as cur:
            cur.execute(operation="SELECT * FROM orders LIMIT 1000000")
            reader = cur.fetch_record_batch()
            writer = pq.ParquetWriter(where="orders.parquet", schema=reader.schema)
            total_rows: int = 0
            for batch in reader:
                writer.write_batch(batch=batch)
                total_rows += batch.num_rows
                print(f"Wrote batch of {batch.num_rows:,d} row(s) - total row(s) written thus far: {total_rows:,d}")

            print(f"Total number of rows written: {total_rows:,d}")


if __name__ == "__main__":
    main()
