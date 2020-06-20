# International Tourists Arrivals into USA Airports

__Data Engineer:__ Lysmar Freitas

#### Project Summary
A Car Rental Company that operates at USA airports hired me to develop a data model in order to do fast data analysis about the international tourists arrivals into the country airport cities in the last years. Some of the main interests with the analysis is to know tourists origin countries and their ammount, tourists travel reasons and the average time they spent during their stay.


The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 

The goal of this project is to pull data from two sources, and create a fact and dimension tables to show international travelers patterns that arrives into USA cities airports. 

Spark was chosen to develop this project for processing large amount of data fast, scale easily with additional worker nodes, with ability to digest different data formats (e.g. SAS, Parquet, CSV).

#### Describe and Gather Data 
To develop the project, it was used the following datasets:

- __I94 Immigration Data:__ this dataset comes from the US National Tourism and Trade Office and includes details on incoming immigrants and their ports of entry. A dataset that includes flight passenger data collected at immigration, such as the airport, arrival and departure, birthyear, gender, airline, etc.
https://travel.trade.gov/research/reports/i94/historical/2016.html
 

- __Airport Code Table:__ this dataset comes from datahub.io and includes airport codes and corresponding cities.
https://datahub.io/core/airport-codes#data

### Step 2: Explore and Assess the Data
#### Explore the Data 
Identifying data quality issues, like missing values, duplicate data, etc.

##### I94 Immigration Data
###### Findings

__Airport Code Table__ 
###### Findings

#### Cleaning Steps

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Mapping out the conceptual data model and explain it 

The data model is a __star schema__ with a fact table (tourist_arrivals) and two dimension tables (immigration and airports) that can be joined onto it. Possible JOINs are indicated with -> other_table.column_name.
 
This schema was chosen because of it's simple to build, visualize and to perform queries to make data analytics the car rental company needs.


__Fact Table__  

tourist_arrivals:
- year
- month
- airport_code
- admission_number
- travel_reason
- stay_duration

__Dimension Tables__  

immigration:
- year
- month
- admission_number >> tourist_arrivals.admission_number
- origin_country
- airport_code
- state_code
- travel_reason
- birth_year
- gender
- stay_duration


airports:
- airport_name
- airpot_code >> tourist_arrivals.airport_code
- type
- city
- state_code
- elevation_ft

#### 3.2 Mapping Out Data Pipelines
Listing the steps necessary to pipeline the data into the chosen data model
- Clean the data on nulls, data types, duplicates, relevance, etc
- Create immigration and airports dimension tables respecivaly from df_immi and df_airport
- Create air_arrivals fact table from df_immi 
- Save processed dimension and fact tables in parquet for query

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.

#### 4.2 Data Quality Checks
 
Run Quality Checks


#### 4.3 Data dictionary 

#### 4.4 Query examples from Data Model

### Step 5: Complete Project Write Up
1. To develop this project, I used Apache Spark to read, transform, create and load data in order to make data analysis to the Car Rental Company possible. The reason for this was due to the small amount of data, the size of the data model (star schema with 3 tables)  and the speed of Spark.
2. The data should be updated annually, as soon as they are available by the resources organizations, since tourist behaviors can change differently by destinations (cities)
3. Under the following scenarios, I would approach the problem differently:
- __In case the data was increased by 100x__,  Apache Hadoop could be used to create a distributed processing system for faster processing.
- __In case the data needs to populate a dashboard that must be updated on a daily basis by 7am every day__, Apache Airflow could be used to create a schedule to run a distributed update on all tables with data streamed from the source.
- __In case the database needed to be accessed by 100+ people__, the project could be hosted in a solution in production scale data warehouse in the cloud, like  Amazon AWS, with larger capacity to serve a lot of users, and workload management to ensure equitable usage of resources across users.
