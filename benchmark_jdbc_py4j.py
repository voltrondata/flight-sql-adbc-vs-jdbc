import os
from pathlib import Path
from py4j.java_gateway import JavaGateway
from utils import Timer, TIMER_TEXT, NUMBER_OF_RUNS, FlightDatabaseConnection, FLIGHT_DB, BENCHMARK_SQL_STATEMENT

SCRIPT_DIR = Path(__file__).parent.resolve()


def benchmark_jdbc_py4j(db: FlightDatabaseConnection = FLIGHT_DB,
                        query: str = BENCHMARK_SQL_STATEMENT
                        ):
    with Timer(name=f"\nJDBC - Py4J - Fetch data from lineitem table",
               text=TIMER_TEXT,
               initial_text=True
               ):
        # Open JVM interface with the JDBC Jar
        jdbc_jar_path = SCRIPT_DIR / "drivers" / "flight-sql-jdbc-driver-13.0.0.jar"
        os.environ["_JAVA_OPTIONS"] = '--add-opens=java.base/java.nio=ALL-UNNAMED'
        gateway = JavaGateway.launch_gateway(classpath=jdbc_jar_path.as_posix())

        # Load the JDBC Jar
        jdbc_class = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
        gateway.jvm.Class.forName(jdbc_class)

        # Initiate connection
        jdbc_uri = (f"jdbc:arrow-flight-sql://{db.hostname}:{str(db.port)}?"
                    "useEncryption=true"
                    f"&user={db.username}&password={db.password}"
                    f"&disableCertificateVerification={str(db.disableCertificateVerification).lower()}"
                    )
        con = gateway.jvm.java.sql.DriverManager.getConnection(jdbc_uri)

        stmt = con.prepareStatement(query)
        rs = stmt.executeQuery()
        stmt.setFetchSize(10000)

        metadata = rs.getMetaData()
        columnCount = metadata.getColumnCount()

        row_count = 0
        while rs.next():
            row_count += 1
        rs.close()
        stmt.close()
        con.close()

        print(f"Number of rows fetched: {row_count}")


if __name__ == "__main__":
    import timeit

    total_time = timeit.timeit(stmt="benchmark_jdbc_py4j()",
                               setup="from __main__ import benchmark_jdbc_py4j",
                               number=NUMBER_OF_RUNS
                               )

    print((f"Number of runs: {NUMBER_OF_RUNS}\n"
           f"Total time: {total_time}\n"
           f"Average time: {total_time / float(NUMBER_OF_RUNS)}"
           )
          )
