## Follow up for the first session

# Tasks for the Red Wine Quality Prediction Project

by The Data Vintners

## Follow-up session 1

### Objective:

Demonstrate the functionality of the Webapp, API, and database components, including data preparation, ingestion pipeline setup, and handling data issues.

### Tasks:

1. **Webapp - API - DB Components Demo:** [COMPLETE]

   - Set up the necessary infrastructure for the Webapp, API, and database components.
   - Ensure that the Webapp can communicate with the API and that the API can interact with the database.

<<<<<<< HEAD
2. **Data Preparation for Ingestion Job:** [COMPLETE]
=======
2. **Script for Data Error Generation:** [COMPLETE]

   - Create a Python script or notebook to generate data errors on the files to be used for ingestion.
   - The script should take the dataset and create errors in random rows.
   
3. **Data Preparation for Ingestion Job:** [COMPLETE]
>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0

   - Prepare the dataset for ingestion by splitting it into multiple files.
   - Store the split files in a folder named `raw-data`, which will serve as the input for the ingestion job.

<<<<<<< HEAD
3. **Script for Data Issue Generation:** [ONGOING]

   - Develop a script with task `read data` to read from _raw-data_ and return filepath.
   - Develop a script with task `save data` to move the _raw-data_ to _good-data_.

4. **Script to Generate Data for Ingestion Job:**

   - Create a Python script to generate data for the ingestion job.
   - The script should take the dataset path, the path of the `raw-data` folder, and the number of files to generate as input parameters.

5. **Simple Ingestion Pipeline Setup:**
   - Develop an Airflow DAG for ingesting data from the `raw-data` folder to the `good-data` folder.
   - The DAG should consist of two tasks:
     - `read-data`: Randomly selects one file from `raw-data` and returns the file path.
     - `save-file`: Moves the selected file from `raw-data` to `good-data`.
=======
4. **Simple Ingestion Pipeline Setup:** [COMPLETE]
   - Develop an Airflow DAG for ingesting data from the `raw_data` folder to the `good_data` folder.
   - The DAG should consist of two tasks:
     - `read-data`: Randomly selects one file from `raw_data` and returns the file path.
     - `save-file`: Moves the selected file from `raw_data` to `good_data`.

5. **Testing:** [ONGOING]
   - Conduct endpoint testing for ingestion using multiple files.
>>>>>>> 918a146816d2cb94bff7d8b184e2bc15677387e0
