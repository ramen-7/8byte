## Dockerized Stock Data Pipeline (Airflow + Postgres)

This project provides a Dockerized data pipeline that fetches daily stock market data from Alpha Vantage, parses the JSON response, and upserts it into PostgreSQL. Airflow orchestrates the workflow with robust error handling and retries.

### Features
- Airflow (LocalExecutor) orchestrating a single-task DAG to fetch and store data
- Alpha Vantage TIME_SERIES_DAILY API via `requests`
- PostgreSQL target with idempotent upserts and indexes
- Docker Compose single-command bring-up
- Environment-variable based secrets and configuration
- Backoff/retry for API and transient failures

### Prerequisites
- Docker Desktop (with WSL2 on Windows)
- Internet access to reach `alphavantage.co`

### Quick Start
1) Create a `.env` file in the repository root with values like:

```
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
ALPHAVANTAGE_API_KEY=YOUR_ALPHA_VANTAGE_KEY
SCHEDULE_INTERVAL=@daily
STOCK_SYMBOL=MSFT

STOCK_DB_HOST=postgres-stock
STOCK_DB_PORT=5432
STOCK_DB_NAME=stocks
STOCK_DB_USER=stock_user
STOCK_DB_PASSWORD=stock_password
```

2) Start the stack:
```
docker compose up -d --build
```

4) Create or reset the Airflow admin user (run once per new folder/machine):

Linux/macOS (bash/zsh):
```
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

Windows PowerShell (one-line):
```
docker compose exec airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

5) Open Airflow UI at `http://localhost:8080` and log in with:
- Username: `admin`
- Password: `admin`

6) Trigger the DAG `stock_pipeline_dag` manually for the first run, or wait for the schedule.

6) Inspect data in Postgres:
```
docker exec -it postgres-stock psql -U "$env:STOCK_DB_USER" -d "$env:STOCK_DB_NAME"

-- then: SELECT * FROM public.stock_prices ORDER BY ts DESC LIMIT 10;
```

### Architecture
- `docker-compose.yml`: Orchestrates services: `postgres-airflow`, `postgres-stock`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`.
- `docker/airflow/Dockerfile`: Custom Airflow image that installs `requests`, `psycopg2-binary`, and `tenacity`.
- `db/init_stock.sql`: Creates `public.stock_prices` with primary key `(symbol, ts)` and an update trigger for `updated_at`.
- `include/fetch_and_store.py`: Fetches Alpha Vantage data, parses JSON, and performs batched upserts.
- `dags/stock_pipeline_dag.py`: Airflow DAG to schedule and call the pipeline.

### Configuration
- API key: `ALPHAVANTAGE_API_KEY` must be set.
- Symbol: `STOCK_SYMBOL` (default `MSFT`).
- Schedule: `SCHEDULE_INTERVAL` (default `@daily`). You can set cron, e.g., `0 * * * *` for hourly.
- Database: `STOCK_DB_*` variables define the target Postgres connection. Inside Docker, Airflow connects to host `postgres-stock:5432`.

### Error Handling & Resilience
- API requests use exponential backoff and retry (up to 5 attempts) and handle Alpha Vantage rate limit "Note" responses by backing off then retrying.
- JSON parsing tolerates missing/invalid numeric fields.
- Database operations are transactional; failures roll back safely.
- Upserts ensure idempotency on `(symbol, ts)`.


### Scalability
- Add more symbols by scheduling multiple DAG runs with different `STOCK_SYMBOL` environment variables or by extending the DAG to iterate over a list. LocalExecutor supports parallel tasks given adequate resources.
- Switch to a network Postgres or managed database by updating `STOCK_DB_HOST` and related vars.

### Common Commands
- View logs: `docker compose logs -f airflow-scheduler` or check Airflow task logs in the UI.
- Restart services: `docker compose restart airflow-scheduler airflow-webserver`.
- Tear down: `docker compose down -v` (removes volumes, including databases).

### If you moved the project to a new folder and can't log in
When you relocate the repo or set it up in a new directory, Airflow's database and user may not exist yet. Run these in order:
```
docker compose up -d --build
docker compose exec airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

If you want to completely reset everything (warning: deletes data):
```
docker compose down -v
docker compose up -d --build
docker compose exec airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```


### Notes
- Alpha Vantage free tier has strict rate limits. Consider `outputsize=full` or paid plans for larger histories, or swap provider logic in `include/fetch_and_store.py`.
- Time zone: Daily bars are stored using the API-provided date (no timezone). Adjust as needed.

### File Tree (key files)
```
docker-compose.yml
docker/
  airflow/
    Dockerfile
    requirements.txt
dags/
  stock_pipeline_dag.py
include/
  fetch_and_store.py
db/
  init_stock.sql
```


