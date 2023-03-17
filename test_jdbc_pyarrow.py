# This script was inspired by: https://uwekorn.com/2020/12/30/fast-jdbc-revisited.html
import os
import sys
from pathlib import Path

import jpype.imports
import pyarrow.jvm
from dotenv import load_dotenv
from utils import Timer, TIMER_TEXT

with Timer(name=f"\nJDBC - PyArrow - Fetch data from lineitem table", text=TIMER_TEXT):
    # Load our environment for the password...
    load_dotenv(dotenv_path=".env")

    flight_password = os.environ["FLIGHT_PASSWORD"]

    SCRIPT_DIR = Path(__file__).parent.resolve()

    classpath = SCRIPT_DIR / "drivers" / "arrow-flight-sql-combined-jdbc-0.1-SNAPSHOT-jar-with-dependencies.jar"
    os.environ["_JAVA_OPTIONS"] = '--add-opens=java.base/java.nio=ALL-UNNAMED'

    # Start the JVM
    jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={classpath}")

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
    jdbc_uri = ("jdbc:arrow-flight-sql://localhost:31337?"
                "useEncryption=true"
                f"&user=flight_username&password={flight_password}"
                "&disableCertificateVerification=true"
                )

    conn = DriverManager.getConnection(jdbc_uri)

    # We cast DECIMALs to FLOATs to avoid a PyArrow error
    query = """SELECT l_orderkey
     , l_partkey
     , l_suppkey
     , l_linenumber
     , l_quantity
     , CAST (l_extendedprice AS float) AS l_extendedprice
     , CAST (l_discount AS float) AS l_discount
     , CAST (l_tax AS float) AS l_tax
     , l_returnflag
     , l_linestatus
     , l_shipdate
     , l_commitdate
     , l_receiptdate
     , l_shipinstruct
     , l_shipmode
     , l_comment
     FROM lineitem"""

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
