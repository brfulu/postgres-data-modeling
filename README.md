# postgres-data-modeling
### Udacity Data Engineer Nanodegree project
An imaginery startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis. The task is to create a star schema for Postgres and develop an ETL pipleine which will transfer the data from local files to the database.

### Requirements for running locally
- Python3 
- Docker
- Docker-Compose 

### Project structure explanation
```
postgres-data-modeling
│   README.md                # Project description
│   docker-compose.yml       # Postgres container description   
│
└───data                     # The dataset
|   |               
│   └───log_data
│   |   │  ...
|   └───song_data
│       │  ...
│   
└───src                     # Source code
|   |               
│   └───notebooks           # Jupyter notebooks
│   |   │  etl.ipynb        # ETL helper notebook
|   |   |  test.ipynb       # Psql queries notebook
|   |   |
|   └───scripts
│       │  create_tables.py # Schema creation script
|       |  etl.py           # ETL script
|       |  sql_queries.py   # Definition of all sql queries
```

### Instructions for running locally

Clone repository to local machine
```
git clone https://github.com/brfulu/postgres-data-modeling.git
```

Change directory to local repository
```
cd path/to/local/repo
```

Create python virtual environment
```
python3 -m venv venv  # create virtualenv
source venv/bin/activate  # activate virtualenv
pip install -r requirements.txt # install requirements
```

Start postgres container
```
docker-compose up
```

Run scripts
```
cd src/
python -m scripts.create_tables  # create schema
python -m scripts.etl  # load data
```