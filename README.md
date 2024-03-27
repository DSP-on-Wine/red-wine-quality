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

5. **Integration with FastAPI** [COMPLETE]

   - Implement API endpoints for model prediction. [COMPLETE]
   - Implement API to get past predictions.
   - Test API locally.

6. **Streamlit Dashboard Development**

   - Create a dashboard for data visualization and model predictions.
   - Include interactive components.

7. **PostgreSQL Database Integration**

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


## Installation

Follow these steps to set up and install the project:

1. **Clone the repository:** https://github.com/DSP-on-Wine/red-wine-quality.git

2. **Navigate to the project directory:** `cd red-wine-quality`
   

### Install Dependencies

1. Make sure you have Python and pip installed on your system.

2. Install the required dependencies using pip: `pip install -r requirements.txt`


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





## Contributors

### The Data Vintners

- [Bemnet Assefa](https://github.com/Beemnet)
- [Zeineb Rania Labidi](https://github.com/ZeinebRania)
- [Riwa Masaad](https://github.com/Masaad-Riwa)
- [Aichen Sun](https://github.com/as5419)
- [Chorten Tsomo Tamang](https://github.com/Chorten-Tsomo)
