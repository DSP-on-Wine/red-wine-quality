# Red Wine Quality Prediction Project

This project aims to predict the quality of red wine based on various physicochemical properties. The project will involve exploratory data analysis (EDA), baseline model development, feature engineering, model improvement, integration with FastAPI, Streamlit dashboard development, PostgreSQL database integration, Grafana dashboard creation, workflow management using Apache Airflow, and deployment of the machine learning model.

## Project Overview

The goal of this project is to develop a machine learning model that accurately predicts the quality of red wine based on its physicochemical properties. The project will follow a structured approach, starting from data exploration and preprocessing, model development, integration with various technologies, and deployment in a production-like environment.

## Technologies Used

- Python (programming language)
- Pandas, NumPy, Matplotlib, Seaborn (libraries for data manipulation and visualization)
- Scikit-learn (machine learning library)
- FastAPI (framework for building APIs)
- Streamlit (framework for building data-driven web applications)
- PostgreSQL (relational database management system)
- Grafana (open-source analytics and monitoring platform)
- Apache Airflow (workflow management platform)

## Project Plan

1. **Exploratory Data Analysis (EDA)** [COMPLETE]

   - Explore the dataset.
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

5. **Integration with FastAPI**

   - Implement API endpoints for model prediction. [COMPLETE]
   - Implement API to get past predictions.
   - Test API locally.

6. **Streamlit Dashboard Development** [COMPLETE]

   - Create a dashboard for data visualization and model predictions. 
   - Create a dashboard for viewing past predictions
   - Include interactive components.

7. **PostgreSQL Database Integration** [COMPLETE]

   - Set up a PostgreSQL database.
   - Load dataset into the database.
   - Write SQL queries for data retrieval.
  
8. **Grafana Dashboard Creation**

   - Configure Grafana to connect with PostgreSQL.
   - Develop dashboards for monitoring data and model performance.

9. **Workflow Management with Apache Airflow**

   - Design and schedule workflows using Apache Airflow.
   - Monitor and manage workflow execution.

10. **Integration Testing and Deployment**
    - Conduct integration testing.
    - Deploy the application in a production-like environment.
    - Monitor application performance.

**First project demo**: For the requirements of first defence of the project, refer to the `Follow-up Session 1` section of the ReadMe.

## Installation

Follow these steps to set up and install the project:

1. **Clone the repository:** https://github.com/DSP-on-Wine/red-wine-quality.git

2. **Navigate to the project directory:** `cd red-wine-quality`

### Install Dependencies

1. Make sure you have Python and pip installed on your system.

2. Install the required dependencies using pip: `pip install -r requirements.txt`


### Setting Up PostgreSQL Database

To set up the PostgreSQL database for the project, follow these steps:

1. **Installation:**

   - Go to [PostgreSQL Download Page](https://www.postgresql.org/download/) and download the appropriate installer for your operating system.
   - Follow the on-screen installation instructions.
   - During installation, you will be prompted to create a new admin password. 
   - Leave the connection port as 5432.

2. **Create a New PostgreSQL Server:**

   - Open pgAdmin 4.
   - Enter the admin password you input during installation.
   - Create a new server named `wine_quality_local_server`.
   - In the Connection tab, enter the Host name/address as `localhost` and keep the port as `5432`.
   - Default username: `postgres`
   - Password: admin password specified during installation.
   - Save the settings.

3. **Create a New Database:**

   - Right-click on Databases under the `wine_quality_local_server` and choose Create -> Database.
   - Name the database: `wine_quality_predictions`.
   - Save the settings.

4. **Create Predictions Table:**

   - Open the query tool and execute the following script to create the predictions table:

     ```sql
     CREATE TABLE IF NOT EXISTS predictions (
         id SERIAL PRIMARY KEY,
         fixed_acidity FLOAT,
         volatile_acidity FLOAT,
         citric_acid FLOAT,
         residual_sugar FLOAT,
         chlorides FLOAT,
         free_sulfur_dioxide FLOAT,
         total_sulfur_dioxide FLOAT,
         density FLOAT,
         pH FLOAT,
         sulphates FLOAT,
         alcohol FLOAT,
         prediction FLOAT,
         timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
     );
     ```

5. **Update DATABASE_URL in __init__.py in folder fastapi_app:**

   - In the `__init__.py` file, make sure to update the `DATABASE_URL` variable with the correct connection details:

     ```python
     DATABASE_URL = "postgresql://username:password@host:port/databasename"
     # Example:
     DATABASE_URL = "postgresql://postgres:123@localhost:5432/wine_quality_predictions"
     ```

6. **Run the FastAPI Notebook:**

   - If you have the fastapi app running, run the `fastapi.ipynb` notebook to save a prediction in the database. If not, follow the following section on how to launch the fastapi app.
  
   We will not be using the `postgreSQL.ipynb` notebook for this exercise.

By following these steps, you can effectively set up the PostgreSQL database for the project.


## Testing the API with Sample Data

To test the FastAPI API with sample data, you can use the provided Jupyter notebook `fastapi.ipynb`. This notebook sends a POST request to the `/predict/` endpoint with sample input data and displays the prediction received from the API response.

Follow these steps to test the API using the notebook:

1. **Run the Server:**

   Before testing the API, ensure that your FastAPI server is running locally. If you haven't started the server yet, follow these steps:

   - Open a terminal or command prompt.
   - Navigate to the directory containing your FastAPI application script (e.g., `main.py`).
   - Run the following command to start the FastAPI server:

     ```bash
     uvicorn fastapi_app.main:app --reload
     ```

     This command starts the FastAPI server with automatic reloading enabled, allowing you to make changes to the code and see the effects without restarting the server manually.

2. **Open the Jupyter Notebook:**

   Open the `fastapi.ipynb` notebook in your Jupyter environment.

3. **Execute the Notebook Cells:**

   Execute the cells in the notebook sequentially to send a POST request to the FastAPI server with sample input data.

4. **Check the Prediction:**

   After executing the notebook cells, the notebook will display the prediction returned by the FastAPI server.

5. **Review the Results:**

   Review the prediction obtained from the FastAPI server to ensure that it aligns with the expected output.

6. **Experiment with Different Data:**

   Feel free to experiment with different sample data by modifying the values in the `sample_data` dictionary and re-executing the notebook cells.

By following these steps, you can effectively test the FastAPI API with sample data using the provided notebook.



## Running the Streamlit App

Once the dependencies are installed, you can run the Streamlit app using the following command:

```bash
streamlit run streamlit_app/main.py
```

This will start the Streamlit server, and you can access the app in your web browser at http://localhost:8501.

You should now be able to view the dataset used for training the model, and input details of new wine to get predictions.


## Follow-up session 1

### Objective:
Demonstrate the functionality of the Webapp, API, and database components, including data preparation, ingestion pipeline setup, and handling data issues.

### Tasks:

1. **Webapp - API - DB Components Demo:** [COMPLETE]
   - Set up the necessary infrastructure for the Webapp, API, and database components.
   - Ensure that the Webapp can communicate with the API and that the API can interact with the database.

2. **Script for Data Issue Generation:** [ONGOING]
   - Develop a script to introduce data issues in the dataset.
   - The script should be capable of generating various types of data errors, as per project instructions.

3. **Data Preparation for Ingestion Job:** [ONGOING]
   - Prepare the dataset for ingestion by splitting it into multiple files.
   - Store the split files in a folder named `raw-data`, which will serve as the input for the ingestion job.

4. **Script to Generate Data for Ingestion Job:**
   - Create a Python script to generate data for the ingestion job.
   - The script should take the dataset path, the path of the `raw-data` folder, and the number of files to generate as input parameters.

5. **Simple Ingestion Pipeline Setup:**
   - Develop an Airflow DAG for ingesting data from the `raw-data` folder to the `good-data` folder.
   - Initially, the DAG should consist of two tasks:
     - `read-data`: Randomly selects one file from `raw-data` and returns the file path.
     - `save-file`: Moves the selected file from `raw-data` to `good-data`.

## Contributors

### The Data Vintners

- [Bemnet Assefa](https://github.com/Beemnet)
- [Zeineb Rania Labidi](https://github.com/ZeinebRania)
- [Riwa Masaad](https://github.com/Masaad-Riwa)
- [Aichen Sun](https://github.com/as5419)
- [Chorten Tsomo Tamang](https://github.com/Chorten-Tsomo)
