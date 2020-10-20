# airflow-etl-template

Template of an automated ETL pipeline to load data into a Redshift database.  
The template is based on a Airflow project,  
designed for the Data Engineering Nanodegree of Udacity.


## Design
This is a skeleton of an automated data pipeline:
It is composed by 2 modules:
The former implements a data warehouse in Redshift,
the latter automate the task execution with Airflow.

The raw data are stored in a S3 bucket while  
the data warehouse is hosted in Amazon Redshift.

The **staging** part consists in 2 tables:
- staging_songs, that stores the song records
- staging_events, that stores the log records

The **analytics** part has been implemented using a star schema with 5 tables:
- 1 Fact table (songplays)
- 4 Dimension tables (songs, artists, users, time)

## Installation

Before to start:  
Add the current project folder path to PYTHONPATH   
and the location of airflow's dags and config file. 
In ~/.bashrc, append: 
```
PYTHONPATH=your/path/to/repo:$PYTHONPATH 
export PYTHONPATH
export AIRFLOW_HOME=~path/to/dag/directory
export AIRFLOW_CONFIG=~path/to/config/airflow.cfg
```
e.g.
```
PYTHONPATH=~/PycharmProjects/airflow-etl-template:$PYTHONPATH 
export PYTHONPATH
export AIRFLOW_HOME=~/PycharmProjects/airflow-etl-template/airflow_etl_template/airflow/src
export AIRFLOW_CONFIG=~/PycharmProjects/airflow-etl-template/config/airflow.cfg
```

To install and activate the environment:
```
conda env create -f airflow_etl_template/environment.yml
conda activate airflow_etl_template 
```

To use this software an aws account is needed.  
Also, a configuration file named *dwh_launch.cfg* 
has to be created under *airflow-etl-template/credentials*.  
it has to contain the aws credentials and the infrastructure configuration,
with the following format:
```
[AWS]
KEY=YoUr_AwS_Key
SECRET=YoUr_sEcReT_aWs_pAsSwOrD

[HARDWARE]
CLUSTER_TYPE=multi-node
CLUSTER_NUM_NODES=4
CLUSTER_NODE_TYPE=dc2.large
CLUSTER_IDENTIFIER=dwhCluster

[DATABASE]
DB_NAME=dwh
DB_USER=dwhuser
DB_PASSWORD=Passw0rd
DB_PORT=5439

[SECURITY]
DWH_IAM_ROLE_NAME=dwhRole
DWH_SECURITY_GROUP_ID=sg-292b4502
```

## Data Warehouse module - Usage
To create the infrastructure:
```
python airflow_etl_template/redshift/scripts/create_infrastructure.py
```

To drop the current tables and create new empty ones:
```
python airflow_etl_template/redshift/scripts/create_tables.py
```

To run the etl pipeline on the full data from s3:
```
python airflow_etl_template/redshift/scripts/etl.py
```

To check the content of the database, run:
```
python airflow_etl_template/redshift/scripts/check_database.py
```

## Redshift Modules - Tests
To run all unittests:
```
python -m unittest discover airflow_etl_template/redshift/tests
```

## Automation Module - Usage
First of all,  
store the connection profiles (of redshift and aws) inside airflow:
```
python airflow_etl_template/airflow/scripts/setup_connections.py
```

Then, create the redshift tables from scratch:
```
python airflow_etl_template/redshift/scripts/create_tables.py
```

Finally, to run the automated pipeline, launch airflow with:
```
sh airflow_etl_template/airflow/scripts/spin_airflow.sh
```
and turn the dag from OFF to ON, in the airflow UI:
```
http://localhost:8080
```

## Automation Module - Tests 
To run all unittests:
```
python -m unittest discover airflow_etl_template/airflow/tests
```

## IMPORTANT NOTE
when the task is finished,  
remember to delete the aws infrastructure with:
```
python airflow_etl_template/redshift/scripts/delete_infrastructure.py
```
and to shutdown the server with:
```
pkill airflow
```
