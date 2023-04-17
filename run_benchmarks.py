# This script was inspired by https://github.com/0x0L/pgeon/blob/main/benchmarks/run.py
from benchmark_adbc import benchmark_adbc
from benchmark_jdbc_py4j import benchmark_jdbc_py4j
from benchmark_jdbc_super_jar import benchmark_jdbc_super_jar
from benchmark_duckdb import benchmark_duckdb
from utils import FlightDatabaseConnection, FLIGHT_DB, BENCHMARK_SQL_STATEMENT, NUMBER_OF_RUNS
import time
import pandas as pd
import seaborn as sns
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
GRAPH_OUTPUT_DIR = SCRIPT_DIR / "graph_output"


def benchmark_callable(func: callable, **kwargs) -> callable:
    def fn():
        func(**kwargs)

    return fn


def benchmark(fn, n=1):
    elapsed = []
    for _ in range(n):
        start = time.time()
        _ = fn()
        elapsed.append(time.time() - start)
    return elapsed


def bench_minute_bars(db: FlightDatabaseConnection,
                      query: str,
                      n: int
                      ):
    print("Running minute_bars benchmark...")

    df = {
        "benchmark_jdbc_super_jar": benchmark(benchmark_callable(func=benchmark_jdbc_super_jar, db=db, query=query), n=n),
        "benchmark_adbc": benchmark(benchmark_callable(func=benchmark_adbc, db=db, query=query), n=n),
        "benchmark_jdbc_py4j": benchmark(benchmark_callable(func=benchmark_jdbc_py4j, db=db, query=query), n=n),
        "benchmark_duckdb": benchmark(fn=benchmark_callable(func=benchmark_duckdb, query=query), n=n),
    }

    df = pd.DataFrame(df)
    df.to_csv(GRAPH_OUTPUT_DIR / "minute_bars.csv", index=False)

    ax = sns.kdeplot(data=df, fill=True)
    ax.get_legend().set_frame_on(False)
    ax.figure.set_size_inches(12, 3)
    ax.xaxis.set_label_text("seconds")
    ax.yaxis.set_visible(False)
    sns.despine(left=True)

    ax.figure.tight_layout()
    ax.figure.savefig(GRAPH_OUTPUT_DIR / "minute_bars.svg")  # , transparent=True)


if __name__ == "__main__":
    bench_minute_bars(db=FLIGHT_DB,
                      query=BENCHMARK_SQL_STATEMENT,
                      n=NUMBER_OF_RUNS
                      )
