from codetiming import Timer
from pathlib import Path

# Constants
TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"
BENCHMARK_SQL_STATEMENT = """SELECT l_orderkey
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

SCRIPT_DIR = Path(__file__).parent.resolve()
DATA_DIR = Path("data").resolve()
DUCKDB_DB_FILE = DATA_DIR / "tpch_sf1.duckdb"
