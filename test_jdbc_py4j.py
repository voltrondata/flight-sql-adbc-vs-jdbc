import os
from pathlib import Path
from dotenv import load_dotenv
from py4j.java_gateway import JavaGateway
from utils import Timer, TIMER_TEXT

SCRIPT_DIR = Path(__file__).parent.resolve()

with Timer(name=f"\nJDBC - Py4J - Fetch data from lineitem table", text=TIMER_TEXT):
    # Open JVM interface with the JDBC Jar
    jdbc_jar_path = SCRIPT_DIR / "drivers" / "flight-sql-jdbc-driver-11.0.0.jar"
    os.environ["_JAVA_OPTIONS"] = '--add-opens=java.base/java.nio=ALL-UNNAMED'
    gateway = JavaGateway.launch_gateway(classpath=jdbc_jar_path.as_posix())

    # Load the JDBC Jar
    jdbc_class = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
    gateway.jvm.Class.forName(jdbc_class)

    # Load our environment for the password...
    load_dotenv(dotenv_path=".env")

    flight_password = os.environ["FLIGHT_PASSWORD"]

    # Initiate connection
    jdbc_uri = ("jdbc:arrow-flight-sql://localhost:31337?"
                "useEncryption=true"
                f"&user=flight_username&password={flight_password}"
                "&disableCertificateVerification=true"
                )
    con = gateway.jvm.java.sql.DriverManager.getConnection(jdbc_uri)

    sql = "SELECT * FROM lineitem"
    stmt = con.prepareStatement(sql)
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

    print(f"Fetch a total of: {row_count} row(s)")
