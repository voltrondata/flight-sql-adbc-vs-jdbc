# Arrow Flight SQL - ADBC vs. JDBC

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
# Install requirements
pip install -r ./requirements.txt
```

### Run a Flight SQL Server
See this [repo](https://github.com/voltrondata/flight-sql-server-example) for instructions on how to run Flight SQL in Docker...

### Create a .env file with a FLIGHT_PASSWORD env var in the top directory (change to whatever password you ran the Flight SQL server with)
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
