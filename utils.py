import os
from pathlib import Path
from codetiming import Timer
from dotenv import load_dotenv

# Load our environment file
load_dotenv(dotenv_path=".env")

# Constants
TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"
BENCHMARK_SQL_STATEMENT = """SELECT l_orderkey
     , l_partkey
     , l_suppkey
     , l_linenumber
     , CAST (l_quantity AS float) AS l_quantity
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

NUMBER_OF_RUNS = 10

SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_DIR = SCRIPT_DIR / "data"
DUCKDB_DB_FILE = DATA_DIR / "tpch_sf1.duckdb"


class FlightDatabaseConnection(object):
    hostname: str
    port: int
    username: str
    password: str
    disableCertificateVerification: bool

    def __init__(self, hostname: str, port: int, username: str, password: str, disableCertificateVerification: bool):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.disableCertificateVerification = disableCertificateVerification


FLIGHT_DB = FlightDatabaseConnection(hostname=os.getenv("FLIGHT_HOSTNAME", "localhost"),
                                     port=int(os.getenv("FLIGHT_PORT", 31337)),
                                     username=os.environ["FLIGHT_USERNAME"],
                                     password=os.environ["FLIGHT_PASSWORD"],
                                     disableCertificateVerification=bool(os.getenv("DISABLE_CERTIFICATE_VERIFICATION", "TRUE"))
                                     )
