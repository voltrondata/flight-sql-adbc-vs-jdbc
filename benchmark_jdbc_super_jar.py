# This script was inspired by: https://uwekorn.com/2020/12/30/fast-jdbc-revisited.html
import sys
from pathlib import Path

import pyarrow.jvm
from utils import Timer, TIMER_TEXT, NUMBER_OF_RUNS, FlightDatabaseConnection, FLIGHT_DB, BENCHMARK_SQL_STATEMENT, start_jvm

SCRIPT_DIR = Path(__file__).parent.resolve()


def benchmark_jdbc_super_jar(db: FlightDatabaseConnection = FLIGHT_DB,
                             query: str = BENCHMARK_SQL_STATEMENT
                             ):
    with Timer(name="\nJDBC - PyArrow - Fetch data from lineitem table",
               text=TIMER_TEXT,
               initial_text=True
               ):
        from java.sql import DriverManager

        from org.apache.arrow.adapter.jdbc import JdbcToArrowUtils, JdbcToArrowConfigBuilder
        from org.apache.arrow.memory import RootAllocator
        from org.apache.arrow.vector import VectorSchemaRoot

        ra = RootAllocator(sys.maxsize)
        calendar = JdbcToArrowUtils.getUtcCalendar()
        config_builder = JdbcToArrowConfigBuilder()
        config_builder.setAllocator(ra)
        config_builder.setCalendar(calendar)
        config_builder.setTargetBatchSize(-1)
        pyarrow_jdbc_config = config_builder.build()

        # Get a connection to the Flight SQL database
        jdbc_uri = (f"jdbc:arrow-flight-sql://{db.hostname}:{str(db.port)}?"
                    "useEncryption=true"
                    f"&user={db.username}&password={db.password}"
                    f"&disableCertificateVerification={str(db.disableCertificateVerification).lower()}"
                    )

        conn = DriverManager.getConnection(jdbc_uri)

        stmt = conn.createStatement()
        result_set = stmt.executeQuery(query)

        root = VectorSchemaRoot.create(
            JdbcToArrowUtils.jdbcToArrowSchema(
                result_set.getMetaData(),
                pyarrow_jdbc_config
            ),
            pyarrow_jdbc_config.getAllocator()
        )
        try:
            JdbcToArrowUtils.jdbcToArrowVectors(result_set, root, pyarrow_jdbc_config)
            pyarrow_table = pyarrow.jvm.record_batch(root)
            print(f"Number of rows fetched: {pyarrow_table.num_rows}")
        finally:
            # Ensure that we clear the JVM memory.
            root.clear()
            stmt.close()
            conn.close()


if __name__ == "__main__":
    import timeit

    start_jvm()

    total_time = timeit.timeit(stmt="benchmark_jdbc_super_jar()",
                               setup="from __main__ import benchmark_jdbc_super_jar",
                               number=NUMBER_OF_RUNS
                               )

    print((f"Number of runs: {NUMBER_OF_RUNS}\n"
           f"Total time: {total_time}\n"
           f"Average time: {total_time / float(NUMBER_OF_RUNS)}"
           )
          )
