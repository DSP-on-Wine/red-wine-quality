# Red Wine Quality Prediction Project

This project aims to predict the quality of red wine based on various physicochemical properties.

## Project Overview

The goal is to develop a machine learning model that predicts red wine quality. The project includes EDA, model development, integration with FastAPI, Streamlit dashboard creation, PostgreSQL database integration, Grafana dashboard setup, workflow management using Apache Airflow, and deployment.

## Project Deliverables

- Machine learning model for wine quality prediction
- Streamlit web application for user interaction
- FastAPI endpoint for model integration
- PostgreSQL database for storing predictions
- Grafana dashboards for data visualization
- Apache Airflow DAGs for workflow automation

## Project Plan

1. **Exploratory Data Analysis (EDA)** [COMPLETE]

   - Explore the dataset (.csv format).
   - Visualize distributions and relationships between features.
   - Preprocess the data.

2. **Baseline Model Development** [COMPLETE]

   - Train a baseline model without feature engineering.
   - Evaluate model performance.

3. **Feature Engineering** [COMPLETE]

   - Engineer new features based on EDA insights.
   - Encode categorical variables.
   - Scale numerical features.

4. **Model Improvement** [COMPLETE]

   - Select and tune machine learning algorithms.
   - Evaluate tuned models.
   - Design and implement a Streamlit web application for user interaction with the model.

5. **Integration with FastAPI** [COMPLETE]

   - Implement API endpoints for model prediction. [COMPLETE]
   - Implement API to get past predictions.
   - Test API locally.

6. **Streamlit Dashboard Development** [COMPLETE]

   - Create a dashboard for data visualization and model predictions.
   - Create a dashboard for viewing past predictions.
   - Include interactive components.

7. **PostgreSQL Database Integration** [COMPLETE]

   - Set up a PostgreSQL database.
   - Load dataset into the database.
   - Write SQL queries for data retrieval.
   - Involve with the airflow dags [on]

8. **Grafana Dashboard Creation** [on]

   - Configure Grafana to connect with PostgreSQL.
   - Develop dashboards for monitoring data and model performance.

9. **Workflow Management with Apache Airflow** [ONGOING]

   - Design and schedule workflows using Apache Airflow.
   - Monitor and manage workflow execution.
   - Automate data ingestion and model retraining workflows.

10. **Integration Testing and Deployment**
    - Conduct integration testing.
    - Deploy the application in a production-like environment.
    - Monitor application performance.

**Project Instructions**: For the requirements of this project, please refer to the `README.md` file.

