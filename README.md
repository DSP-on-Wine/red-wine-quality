# Red Wine Quality Prediction Project

This project aims to predict the quality of red wine based on various physicochemical properties. The project will involve exploratory data analysis (EDA), baseline model development, feature engineering, model improvement, integration with FastAPI, Streamlit dashboard development, PostgreSQL database integration, Grafana dashboard creation, workflow management using Apache Airflow, and deployment of the machine learning model.

## Project Overview

The goal of this project is to develop a machine learning model that accurately predicts the quality of red wine based on its physicochemical properties. The project will follow a structured approach, starting from data exploration and preprocessing, model development, integration with various technologies, and deployment in a production-like environment.

## Technologies Used

- Python (programming language)
- Pandas, NumPy, Joblib, Pydantic and other python libraries (full list of dependencies in `requirements.txt`)
- Scikit-learn (machine learning library)
- FastAPI (framework for building APIs)
- Streamlit (framework for building data-driven web applications)
- PostgreSQL (relational database management system)
- Grafana (open-source analytics and monitoring platform)
- Apache Airflow (workflow management platform)

## Installation

Follow these steps to set up and install the project:

1. **Clone the repository:** https://github.com/DSP-on-Wine/red-wine-quality.git

2. **Navigate to the project directory:** `cd red-wine-quality`


### Install Dependencies

1. Make sure you have Python version 3.12 and pip installed on your system.
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
   - Name the database: `wine_quality`.
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

5. **Create a `.env` file:**

   - In the root directory of the project, create a new file named `.env`.
   - Inside the `.env` file, define the following environment variables with your PostgreSQL connection details:

     ```ini
     DB_USER=postgres
     DB_PASSWORD=123
     DB_HOST=localhost
     DB_PORT=5432
     DB_NAME=wine_quality
     ```

   - Replace the values with your actual PostgreSQL connection details. Make sure to not include any quotation marks or spaces around the values.
   - Save the `.env` file in the root directory of your project.

6. **Run the FastAPI Notebook:**

   - If you have the fastAPI app running, run the `fastapi.ipynb` notebook to save a prediction in the database. If not, follow the following section on how to test the FastAPI with sample data.

By following the above steps, you can effectively set up the PostgreSQL database for the project.

### Testing the FastAPI with Sample Data

To test the FastAPI API with sample data, you can use the provided Jupyter notebook `fastapi.ipynb`. This notebook sends a POST request to the `/predict/` endpoint with sample input data and displays the prediction received from the API response.

Follow these steps to test the API using the notebook:

1. **Run the Server:**

   Before testing the API, ensure that your FastAPI server is running locally. If you haven't started the server yet, follow these steps:

   - Open a terminal or command prompt.
   - Navigate to the root directory of your project.
   - Run the following command to start the FastAPI server:

     ```bash
     uvicorn fastapi_app.main:app --reload
     ```

     This command starts the FastAPI server with automatic reloading enabled, allowing you to make changes to the code and see the effects without restarting the server manually.

2. **Open the Jupyter Notebook:**

   Open the `fastapi.ipynb` notebook in your Jupyter environment.

3. **Execute the Notebook Cells:**

   Execute the cells in the notebook sequentially to send a POST request to the FastAPI server with sample input data for both single and batch prediction jobs.

4. **Check the Prediction:**

   After executing the notebook cells, the notebook will display the prediction returned by the FastAPI server.

5. **Review the Results:**

   Review the prediction obtained from the FastAPI server to ensure that it aligns with the expected output.

6. **Experiment with Different Data:**

   Feel free to experiment with different sample data by modifying the values in the `sample_data` dictionary for single predict requests or changing the `test-data.csv` for batch predicting and re-executing the notebook cells.

By following these steps, you can effectively test the FastAPI API with sample data using the provided notebook.

### Running the Streamlit App

Once the fastAPI app is running, you can run the Streamlit app using the following command from the root directory.
Keep the API running, open a new terminal and run the command:

```bash
streamlit run streamlit_app/main.py
```

This will start the Streamlit server, and you can access the app in your web browser at http://localhost:8501.

You are now equipped to perform several actions with the application:

- Explore the dataset used to train the model.
- Input details of new wine to receive predictions.
- Upload a CSV file containing test datasets to obtain batch predictions.
- Review all past predictions made by you within a specific time frame.
  Note: Ensure to adjust the start and end dates in the app according to the time intervals during which you made predictions.

These functionalities allow you to interact comprehensively with the application, from exploring the dataset to using the prediction capabilities, enhancing your overall user experience.

### Data Preparation Using `data_preparation.ipynb`

To prepare the `winequality-red.csv` dataset for ingestion, follow these steps:

1. **Create a `raw_data` Folder:**

   - Navigate to the root directory of the project.
   - Create two folders named `raw_data` and `good_data`. (`good_data` will remain empty for now, we will only be using `raw_data`.)

2. **Ensure CSV Location:**

   - Make sure the `winequality-red.csv` file is present in the `data` folder under root directory of the project.

3. **Open and Run the Notebook:**

   - Open the `data_preparation.ipynb` notebook using your preferred Jupyter Notebook environment.
   - In the notebook, ensure that the file path to `winequality-red.csv` is correctly set to point to its location in your computer. If you are preparing another file, please change the location accordingly.

4. **Run the Notebook Cells:**

   - Execute the cells in the notebook sequentially.
   - The notebook will generate 20 random errors within the `winequality-red.csv` dataset. It will then split the dataset by randomly selecting 10 rows at a time and stores each split in separate CSV files within the `raw_data` folder.

5. **Verify Output:**

   - After running all cells, check the `raw_data` folder to confirm that the dataset has been split and stored correctly.
   - Each CSV file in the `raw_data` folder will contain a subset of the original dataset with 10 rows per file. You can validate that the files each contain 10 rows of data, and there are the correct number of new files created.
   - If you are splitting the `winequality-red.csv` dataset, you should now have 160 files with 10 rows of data each.

By following these steps, you can effectively prepare the dataset for ingestion. You can now move on to the next step

### Apache Airflow Docker Installation Guide (for Windows)

This guide will help you set up Apache Airflow with Docker and ingest the `winequality-red.csv` file using the `ingest_data.py` DAG.

#### Prerequisites

Before proceeding with the installation, ensure that you have the following prerequisites:

- **Docker Community Edition (CE)**: Install Docker CE on your Windows system. You can follow the installation guides found [here](https://docs.docker.com/desktop/install/windows-install/).
- **Docker Compose v4.27.0 or newer**: Docker Compose is usually included with Docker Desktop for Windows installations. Ensure that you have a version of Docker Compose that is compatible with your Docker CE installation.
- **At least 4GB of memory allocated for Docker**: Adjust the Docker memory settings to allocate at least 4GB of memory for Docker containers. This can be done through the Docker Desktop settings.

#### Installation Steps

1. **Navigate to the project directory:**

   ```bash
   cd airflow
   ```

   - Make sure that you have the file `docker-compose.yaml` in this directory. If not, please pull from the remote branch and try again.

2. **Create necessary directories:**

   ```bash
   mkdir logs, plugins, config
   ```

3. **Add paths in .env file:**

   - Within the airflow folder, generate a `.env` file and specify the following directories.

   ```bash
   AIRFLOW_IMAGE_NAME=apache/airflow:2.8.4
   AIRFLOW_UID=50000
   RAW_DATA_DIR = '../raw_data'
   GOOD_DATA_DIR = '../good_data'
   ```

   Note that this `.env` file is distinct from the one you created in the root directory, which contains database connection information.

   - Ensure you've previously created the `raw_data` and `good_data` folders during the data preparation phase. If these folders or the files within `raw_data` are not present locally, refer to the **Data Preparation** section above.

   - RAW_DATA_DIR and GOOD_DATA_DIR should specify the paths of the raw_data and good_data folders in your directory.
   - AIRFLOW_IMAGE_NAME and AIRFLOW_UID refer to default values used by docker.

4. **Start Airflow using Docker Compose:**

   ```bash
   docker-compose up -d
   ```

5. **Make sure docker is enabled in the windows firewall.**

6. **Access Airflow web interface:**
   - Once the services are up and running, you can access the Airflow web interface at [http://localhost:8080](http://localhost:8080).
   - Use the following credentials to log in:
     - **Username:** airflow
     - **Password:** airflow

#### Usage

- After launching Airflow, you can start using the DAGs located in the `dags` directory.
- Additionally, you can place your DAG files in the `dags` directory to schedule and execute tasks.

#### Cleanup

- To stop and remove containers, as well as delete volumes with database data, run:
  ```bash
  docker-compose down --volumes --remove-orphans
  ```

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
