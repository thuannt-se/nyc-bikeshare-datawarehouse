# New York citibike ETL pipeline and data warehouse

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

Citibike ETL pipeline and data warehouse is simple ETL tools that utilize the power of Airflow and AWS service to create 
a pipeline for data extraction citibike open data, transform them and store them into datalake for later load them into 
a data warehouse.

## Table of Contents

- [Project Goal](#Project Goal)
- [Technologies why I choose them](#Technologies & why I choose them)
- [Data modeling & dictionary](#Data modeling & dictionary)
- [Data usage](#Data usage)
- [Data Pipeline & update schedule](#Data Pipeline & update schedule)
- [Install](#Install)
- [Usage](#usage)
- [Final Write up](#Final Write up)

## Project Goal
After analyzing the open trip data of the year 2020 from citi bike  
([https://s3.amazonaws.com/tripdata/index.html](https://s3.amazonaws.com/tripdata/index.html)),
I combine the trip data with weather data from NOAA to create a **data warehouse** of citi bike 2020 trip data combine with 
2020 weather data of New York city. User can use this data warehouse to build up a dashboard, create BI report or use this
data warehouse as a source of truth database for any kind of data explore to identity user basis

## Technologies & why I choose them
- **Airflow 2.0**
  - It's the newest Airflow version released on December 2020
  - It has massive Scheduler performance improvements
  - Better UI
  - And more...
  - Find out more at: https://airflow.apache.org/blog/airflow-two-point-oh-is-here/
- AWS S3:
  - Simple but powerful service to store and retrieve large dataset
  - Easy to use and integrate with other AWS service
- AWS EMR cluster _(heavily used  this project)_:
  - Powerful service provided by Amazon to process and transform large dataset.
  - Easy to use, debug error.
  - Already installed Spark package.
- AWS Redshift cluster
  - Cloud database service that optimized for SQL.
  - Very suitable for data warehouse due to its storage capacity and CPU power
  - Redshift has capability to process a large number of simultaneous queries.

## Data modeling & dictionary
Data modeling is described as below image.
![Entity relation diagram](https://github.com/thuannt-se/nyc-bikeshare-datawarehouse/blob/main/resource/citibike-data-warehouse.jpeg)
trip_fact entity would have 2 dimensions entity: dim_datetime, dim_station
![Dictionary](https://github.com/thuannt-se/nyc-bikeshare-datawarehouse/blob/main/resource/trip_fact_dictionary.png)

weather_fact entity would have 1 dimensions entity: dim_datetime and one-to-many relationship with weather_type
![Dictionary](https://github.com/thuannt-se/nyc-bikeshare-datawarehouse/blob/main/resource/weather_fact_dictionary.png)

## Data usage
Using the final database, we can explore the data and understand user behavior on using bike share service in New York City
For example we can answer some questions:
- How many trips are made by New york people make on monthly and yearly basic?
- How gender affect on bike share service usage frequently and number of trip made by male and female.
- How many hours people spent for riding a bike throughout the year?
- Which month has the highest number of bike trips and lowest bike trips?
- How weather affect on user behavior?

## Data Pipeline & update schedule
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
![tripdb](https://github.com/thuannt-se/nyc-bikeshare-datawarehouse/blob/main/resource/Screenshot%202021-09-05%20at%2014-08-11%20Redshift.png)
## Final Write up
1. By utilizing the power of cloud computing (storage, data processing, etc...) it should not be a problem if our data
source increased 100x times in file number. Since we handle extract and transform data by each file. 
2. However, the bottleneck could be the author local machine because it download source data to local machine before upload it to AWS S3
3. The pipelines would be run on a daily basis by 7 am every day: The pipeline is not built for daily basis,
It's suitable to run the pipeline monthly or yearly because it depends on how source data (citibike trip data) is updated
4. Using Redshift, it should not be a problem if the cluster accessed by multiple people at a same time.
5. The number of trip made by user for each month in year are from more than 500k trip to more than 2 million trip. 
![Source](https://github.com/thuannt-se/nyc-bikeshare-datawarehouse/blob/main/resource/Data_row_count_fromJupyterLab.png)
The picture is a number of trips made by New York city people in August 2020 and the pipeline process the data for 2020 year