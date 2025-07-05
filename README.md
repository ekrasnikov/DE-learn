# DE-learn
A project for learning Data Engineering.
This project creates a snapshot pipeline to save daily data...


## Tech Stack
![Python](https://img.shields.io/badge/python-3.12-blue.svg?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE.svg?logo=Apache-Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%232496ED.svg?logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/docker_compose-2496ED?logo=docker&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?logo=postgresql&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-red)


## Project Architecture 
![Project Architecture ](docs/architecture.drawio.svg)


## Getting Started

### Prerequisites

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed on your machine.
* Recommended resources for Docker: at least 4GB RAM and 2 CPUs.

### Installation & Setup

1. **Clone the repository:**
```sh
git clone https://github.com/jinjik19/DE-learn.git
cd DE-learn
```

2. **Setup Environment Variables:**
```sh
# 1. Create the .env file from the example
cp .env.example .env
```
After this, open the `.env` file and add your values (especially your `API_KEY`). You can get a free API key from the [CoinGecko Developers Dashboard](https://www.coingecko.com/en/developers/dashboard).

3. **Build the Application Image:**
```sh
docker build -t de-learn-app:latest .
```

### Running the Application

The environment consists of two parts: the target database and the Airflow infrastructure. It's best to run them in two separate terminal windows.

1. **Run the target database (DWH):**
  ```sh
  docker-compose -f database-compose.yml up -d
  ```

2. **Run the Airflow Infrastructure:**
  ```sh
  docker-compose up
  ```
  > **Note:** The first time you run Airflow, it may take several minutes to initialize.

## Usage

1. Open Airflow web UI in your browser: http://localhost:8080
2. Log in using: **login:** `airflow`, **password:** `airflow`.
3. In the DAGs list, find the DAG with ID `market_data_etl` and activate it by toggling the switch on the left.
4. To trigger the DAG manually, click the **▶️ (Play)** button and select "Trigger DAG". 
5. To check the result, connect to your target PostgreSQL database (port 5432) and run the query:

```sql
SELECT * FROM history_market_data ORDER BY datetime DESC LIMIT 10;
```


## Shutting Down

To stop and remove all containers:

```sh
  # Stop Airflow
  docker-compose down

  # Stop the target database
  docker-compose -f database-compose.yml down
```

