# 4.Data-Engineer - Tiktok - Study Case

## Tiktok

The project aims to build a real-time analytics and sentiment analysis system for TikTok videos using Azure services. This will help businesses and content creators gauge the performance and sentiment of their videos, allowing them to tailor their content strategies and improve audience engagement.

### Data Pipeline Steps

1. Infrastructure: Provision necessary Azure Cloud resources using Terraform.
2. CI/CD: Use GitHub Actions as a CI/CD platform for the Terraform infrastructure.
3. Data Modeling: Use Python to create a script that generates dummy data for the Tiktok project context, simulating user comments and video metadata.
4. Data Ingestion: Utilize Azure Event Hubs to ingest real-time data (video metadata and comments) from the Tiktok API.
5. Data Processing: Employ Azure Stream Analytics to process and analyze streaming data in real-time.
6. Data Lake Raw Zone: Save the data from stream analytics in the Azure Blob Storage Raw Zone.
7. Data Processing: Use Python to handle data transformation and Azure Text Analytics API for sentiment analysis.
8. Data Lake Processing Zone: Store the preprocessed data in Azure Blob Storage Process Zone.
9. Data Warehouse: Load the preprocessed data into Azure Synapse Analytics, which will act as the data warehouse for the project.
10. Data Transformation: Use DBT (Data Build Tool) to transform the data and create a sentiment score view for analysis.
11. Data Orchestration: Orchestrate the data pipeline using Airflow in Docker.

### Pipeline Diagram

![alt text](https://github.com/makima0499/4.Data-Engineer/blob/main/4.DataPipeline.png)

### Tools

* Python
* Airflow
* Terraform
* Github Actions
* Docker
* Azure Event Hubs
* Azure Stream Analytics
* Azure Text Analytics
* Azure Blob Storage
* Azure Synapses Analytics
* DBT

### Note

This repository is provided for study purposes only, focusing on data engineering pipelines.

## License

[MIT](https://choosealicense.com/licenses/mit/)
