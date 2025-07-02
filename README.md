# DE-learn
A project for learning Data Engineering.
This project creates a snapshot pipeline to save daily data...


## Tech Stack
![Python](https://img.shields.io/badge/python-3.12-blue.svg?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%232496ED.svg?logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/docker_compose-2496ED?logo=docker&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?logo=postgresql&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-red)


## Project Architecture 
![Project Architecture ](docs/architecture.drawio.svg)


## Getting Started

### 1. Clone project
```sh
  git clone https://github.com/jinjik19/DE-learn.git
```

### 2. Environment Variables
```
# 1. Copy .env.example file
cp .env.example .env

# 2. Open .env file and insert yours values
```

### 3. Running the Application

```sh
docker-compose up --build
```

## Usage
```
1. Connect to PostgreSQL DB by .env creds
2. Run SQL script: `SELECT * FROM market_chart ORDER BY datetime DESC LIMIT 10;`
```


## Shutting Down
```sh
  docker-compose down
```

