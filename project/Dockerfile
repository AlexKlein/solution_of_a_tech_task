# Use a custom Airflow as base image
FROM puckel/docker-airflow:latest

# Developer and mainteiner of the project
LABEL maintainer="Alexander Klein"

# Switch to ROOT user for installing mandatory packages
USER root

# Install mandatory packages
RUN apt-get update \
 && pip install --upgrade pip \
 && apt-get install -y libpq-dev gcc

# Switch to AIRFLOW user for running the application
USER airflow

# Copy the requirements for the Python app
COPY requirements.txt /usr/local/airflow/app/requirements.txt

# Copy the DAGs for the Airflow app
COPY ./airflow/dags  /usr/local/airflow/dags

# Install needed packages specified in the requirements.txt file and make special directories
RUN pip install --trusted-host pypi.python.org -r /usr/local/airflow/app/requirements.txt \
 && mkdir -p /tmp/etl_project/log \
 && mkdir -p /tmp/etl_project/data

# Copy the current directory contents into the container workdir
COPY . /usr/local/airflow/app

# Run airflow when the container launches
ENTRYPOINT ["/entrypoint.sh"]

# Launch webserver
CMD ["webserver"]
