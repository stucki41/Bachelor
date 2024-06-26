
## Prefect lokal

Die Initiierung Prefect erfordert die Ausführung der nachfolgenden Befehle.

### Start Prefect Server

```bash
prefect server start
```

### Start Agent
```bash
prefect agent start -q 'default'
```

### Cloud Anmeldung
```bash
prefect cloud login
```
## Prefect Docker

Zur Ausführung in Docker muss zunächst die Docker-Umgebung gestartet werden. Anschließend sind die folgenden Befehle auszuführen:
```bash
docker-compose --profile server up
docker-compose run cli
// neu laden mit neuen requirements.txt 
docker-compose build --no-cache cli
```

## Apache Airflow lokal
Die Initiierung Apache Airflow erfordert die Ausführung der nachfolgenden Befehle.

### Installation
```bash
python3 -m venv py_env
pip install 'apache-airflow==2.8.2' --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.10.txt"  
export AIRFLOW_HOME=~/airflow
airflow db init
```

### Start Webserver
```bash
export AIRFLOW_HOME=~/airflow
airflow webserver -p 8080
```
### Start Scheduler
```bash
export AIRFLOW_HOME=~/airflow
airflow scheduler
```

## Apache Airflow Docker
Zur Ausführung in Docker muss zunächst die Docker-Umgebung gestartet werden. Anschließend sind die folgenden Befehle auszuführen:

```bash
docker build . --tag extending_airflow:latest
docker-compose up -d
```
