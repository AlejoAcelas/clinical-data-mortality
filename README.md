# Clinical Data Mortality (In progress)

This repository builds machine learning models to predict 1-year mortality risk scores for patients following the onset of a specified condition, based on their medical history.

The models are trained on synthetic patient data from CMS (Centers for Medicare and Medicaid Services) formatted to the OMOP Common Data Model standard. This standardizes medical terminology and coding across datasets. 

## Pipeline

- Patient data ingestion and transformation using AWS Glue
- Model training and evaluation using Amazon SageMaker 
- Models output 1-year mortality risk forecasts for future patients  

## Tools

- AWS Glue for scalable data integration 
- Amazon SageMaker for portable, distributed model building 
- OMOP CDM for standardized medical data
- Synthetic CMS dataset with 1K - 2.3M patients

This implementation focuses on developing an end-to-end machine learning pipeline for healthcare data leveraging AWS cloud services. The workflow showcases practical skills in cloud-based ETL, distributed training, and implementing ML solutions on medical data.
