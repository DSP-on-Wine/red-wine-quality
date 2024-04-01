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

2. **Script for Data Error Generation:** [COMPLETE]

   - Create a Python script or notebook to generate data errors on the files to be used for ingestion.
   - The script should take the dataset and create errors in random rows.
   
3. **Data Preparation for Ingestion Job:** [COMPLETE]

   - Prepare the dataset for ingestion by splitting it into multiple files.
   - Store the split files in a folder named `raw-data`, which will serve as the input for the ingestion job.

4. **Simple Ingestion Pipeline Setup:** [COMPLETE]
   - Develop an Airflow DAG for ingesting data from the `raw_data` folder to the `good_data` folder.
   - The DAG should consist of two tasks:
     - `read-data`: Randomly selects one file from `raw_data` and returns the file path.
     - `save-file`: Moves the selected file from `raw_data` to `good_data`.

5. **Testing:** [ONGOING]
   - Conduct endpoint testing for ingestion using multiple files.
