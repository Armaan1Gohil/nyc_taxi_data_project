# NYC Taxi Data Engineering Project

# NYC Taxi Data Engineering Project

## Introduction

This project is designed to demonstrate a full data engineering pipeline using NYC taxi data. The project involves several key components, from data ingestion and transformation to storage and analysis, with deployment both locally and on cloud platforms.

### Project Overview

1. **Local Deployment**:
   - **Data Ingestion**: Leveraging `Beautiful Soup (bs4)` for web scraping to gather NYC taxi data.
   - **Data Transformation**: Utilizing `Apache Spark` to perform scalable transformations and processing on the data.
   - **Data Storage**: Storing the transformed data in a `PostgreSQL` database.
   - **Containerization**: All components are containerized using `Docker` to ensure portability and ease of deployment.

2. **Cloud Deployment**:
   - **AWS**: Replicating the entire pipeline on AWS, utilizing services like `Amazon EC2`  for compute instance, `Amazon S3` for data storage, `Amazon EMR` for Spark processing and `Amazon Redshift` for data warehousing.
   - **Google Cloud Platform (GCP)**: Similarly, the project is also deployed on GCP using services like `Google Compute Engine`, `Google Cloud Storage`, `Google Dataproc` and ` GoogleBigQuery`.

This project is an excellent opportunity to explore the end-to-end data engineering process, from raw data acquisition to data processing and storage, while learning to deploy solutions across multiple cloud environments.

## Stepa to Executiond

## STEP - 1
Creating an Entity Relationship Diagram (ERD) consisting of fact table and dimension tables. This separation ensures efficient storage, retrieval, and analysis of large datasets.
![NYC Taxi ERD](https://github.com/Armaan1Gohil/nyc_taxi_data_project/assets/46198340/24ae08b0-b5c6-4e59-b92d-9c25be2cebaa)
