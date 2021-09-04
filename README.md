# New York citibike ETL pipeline and data warehouse

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

Citibike ETL pipeline and data warehouse is simple ETL tools that utilize the power of Airflow and AWS service to create 
a pipeline for data extraction citibike open data, transform them and store them into datalake for later load them into 
a data warehouse.

## Table of Contents

- [Project Goal](#Project Goal
- [Project Goal](#Project Goal)
- [Technology](#Technology)
- [Data Pipeline](#Dag Pipeline)
- [Usage](#usage)
- [API](#api)
- [Contributing](#contributing)
- [License](#license)

## Project Goal
After analyzing the open trip data of the year 2020 from citi bike  
([https://s3.amazonaws.com/tripdata/index.html](https://s3.amazonaws.com/tripdata/index.html)),
I combine the trip data with weather data from NOAA to create a **data warehouse** of citi bike 2020 trip data combine with 
2020 weather data of New York city. User can use this data warehouse to build up a dashboard, create BI report or use this
data warehouse as an source of truth database for any kind of data explore to identity user basis

##Technology:
- Airflow 2.0
- AWS S3
- AWS EMR cluster _(heavily used  this project)_
- AWS Redshift cluster

## Data modeling & 
Data modeling is described as below image.
![Entity relation diagram](https://github.com/thuannt-se/nyc-bikeshare-datawarehouse/blob/main/resource/citibike-data-warehouse.jpeg)
trip_fact entity would have 2 dimensions entity: dim_datetime, dim_station
weather_fact entity would have 1 dimensions entity: dim_datetime and one-to-many relationship with weather_type

## Data Pipeline & update basis
The source data can be updated monthly or yearly. The data pipeline should not depend on the interval of data.

The DAG runs through multiple tasks as below image. 

![Data pipeline](https://github.com/thuannt-se/nyc-bikeshare-datawarehouse/blob/main/resource/dag_pipeline.png)Data pipeline
1. It download NYC Citibike trip data from public s3 bucket and upload trip data and weather data to s3 bucket 
2. Next pipe upload the etl script into s3
3. Run EMR cluster to extract and transform trip data and weather data into csv file and save them into another s3 bucket.
4. Load all the transformed data into redshift cluster.
## Install
**Requirment**: 
- Airflow version >= 2.
- python3

Please make sure that your computer install the custom operators as python package before run airflow
```
#From git folder, navigate to plugins folder
cd plugins 

#run setup.py to create package
python setup.py bdist_wheel

#install package
pip install dist/airflow_custom_operators-0.0.1-py3-none-any.whl
```


## Usage
After installing custom package and Airflow 2.0 successfully, start airflow server by these command

```
airflow db init
airflow scheduler
airflow webserver
```
1. Add AWS connection and Redshift connection to Airflow server 
(Follow the tutorial: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui)
2. Run nyc_bikeshare_datawarehouse dag.

After the pipline finish it's process. You can checkout the result in Redshift cluster.
## Final Write up
1. By utilizing the power of cloud computing (storage, data processing, etc...) it should not be a problem if our data
source increased 100x times in file number. Since we handle extract and transform data by each file. 
2. However, the bottleneck could be author local machine because it download source data before upload it to AWS S3
3. Using Redshift, it should not be a problem if the cluster accessed by multiple people at a same time.