# Solution of the Just Eat Takeaway tech test

In this project I showed my Python and SQL coding skills.

## Description

This repository consists of:

```
- An ETL process for:
    - downloading files from external sources;
    - delivering data from a CSV-file and a JSON-file to the PostgreSQL DB;
    - transforming the data and loading it to core and datamart layers;
- SQL script which creates:
    - database schemas;
    - tables;
- SQL queries for filling data in core and datamart layers;
- Airflow DAG for orchestrate this application;
- Docker files for wrapping this tool;
- Presentation of my solution.
```

## Classes

Also, the project contains several classes, there are:
1. `logger` - for logging actions in the log file;
2. `postgres_wrapper` - for making work with your database easier;
3. `singleton` - for making only one log file in the application.

## Log file

You should make several steps for getting log file.
1. `docker ps -a` - you find container ID;
2. `docker exec -it {container_id} bash` - you get into Airflow docker container;
3. `cat /tmp/etl_project/log/output.log` - you read the log file.

## Airflow
As the orchestrator I choose Airflow. The link for the Airflow UI is http://192.168.99.100:8080/admin/

## Docker-compose

When you need to start the app with all infrastructure, you have to make this steps:
1. Change environment variables in [YML-file](./project/docker-compose.yml) (now the default values are) 
2. Execute the `docker-compose up -d --build` command - in this step the app will be built, tables will be created in the DB and ETL process will be started.

## Note

You should wait a couple of minutes for the database and Airflow webserer start. You can check it with the `docker ps -a` command. 
After that you may run the application and check logs as the next step.

## Potential improvements

- Add a system for working with log files, e.g. Splunk or ELK.
- Add a visualization system, e.g. Tableau or Qlik Sense.
- Cover this application by tests.
- Add metadata management system, e.g. PostgreSQL and Self-written service for:
    - store all DAG runs and its statuses;
    - generate new DAGs in code.
- Parse JSONs more granularity for deep analyse.
