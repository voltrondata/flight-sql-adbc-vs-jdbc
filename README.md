# Arrow Flight SQL - ADBC vs. JDBC
[<img src="https://img.shields.io/badge/dockerhub-Flight%20SQL%20Server%20docker%20image-green.svg?logo=Docker">](https://hub.docker.com/r/voltrondata/superset-sqlalchemy-adbc-flight-sql)
[<img src="https://img.shields.io/badge/GitHub-voltrondata%2Fflight--sql--server--example-blue.svg?logo=Github">](https://github.com/voltrondata/flight-sql-server-example)

This repo is intended to benchmark ADBC and JDBC drivers connecting a client to a running Flight SQL database server.

## Setup

### Clone the repo
```shell
git clone https://github.com/voltrondata/flight-sql-adbc-vs-jdbc
cd flight-sql-adbc-vs-jdbc
```

Create a new Python 3.8+ virtual environment:
```shell
# Create the virtual environment
python3 -m venv ./venv
# Activate the virtual environment
. ./venv/bin/activate
# Update pip
pip install --upgrade pip
# Install requirements
pip install -r ./requirements.txt
```

### Run a Flight SQL Server with a TPC-H Scale Factor 1 (1GB) database

```
# Generate a TPC-H database in the host's /tmp directory
pushd /tmp

duckdb ./tpch_sf1.duckdb << EOF
.bail on
.echo on
SELECT VERSION();
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);
EOF

# Run the flight-sql docker container image - and mount the host's DuckDB database file created above inside the container
docker run --name flight-sql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env FLIGHT_PASSWORD="flight_password" \
           --pull missing \
           --mount type=bind,source=$(pwd),target=/opt/flight_sql/data \
           --env DATABASE_FILE_NAME="tpch_sf1.duckdb" \
           voltrondata/flight-sql:latest

popd
```

For more details - see this [repo](https://github.com/voltrondata/flight-sql-server-example) for instructions on how to run Flight SQL in Docker...

### Create a .env file with a FLIGHT_PASSWORD env var in the repo root directory (change to whatever password you ran the Flight SQL server with)
```shell
echo "FLIGHT_PASSWORD=flight_password" > ./.env
```

## Run benchmarks

### Run ADBC example
```shell
python test_adbc.py
```

### Run JDBC - Py4J example
```shell
python test_jdbc_py4j.py
```

### Run JDBC - PyArrow example
```shell
python test_jdbc_pyarrow.py
```

### Run DuckDB local Database example
```shell
# First create a local TPC-H Scale Factor 1 (1 GB) database 
python create_local_duckdb_database.py
# Now run the benchmark
python test_duckdb.py
```
