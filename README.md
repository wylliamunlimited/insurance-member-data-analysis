# Insurance Member Data Pipeline

Dataset: Member Rosters of Insurances

## Goals
1. Deliver a data pipeline for **scalable ingestion of data**, considered case -> **biweekly updates of new data**
2. Report a **summary of datasets** (customized for PM use case) for each update
3. Propose scalable methods for **triggering of Python script whenever data is updated**

## Installation

### Files Required
Put the database file inside the root directory. Make sure the name is `n1_data_ops_challenge.db`. 

### Conda Environment Set Up \[Optional\]
1. Create a **Conda** environment using the YAML configuration file
```conda env create -f environment.yml```
2. Configure kernel for Jupyter (in case of notebook revisits)
```python3 -m ipykernel install --user --name member-pipeline --display-name "Python 3 (member-pipeline)"```
    * When opening jupyter notebook, change the runtime kernel to `Python 3 (member-pipeline)`

### Pip Set Up
Install all required Python packages by:
```pip install -r requirements.txt```

### Notebook Usage
I used **jupyterlab** for all `.ipynb` files. Simply do `jupyter lab` at root directory. 

## Footprint
1. **Exploration of data** (Refer to `eda.ipynb`)
    * Import data as Pandas DataFrame
    * Get meta-data for each roster dataset & model score dataset
    * Check columns for each roster dataset - all schemas are consistent
    * Check for Null values -> no null value found
    * Sample the dataset for content discrepency
       * `roster_4` uses state abbreviations; others do not
       * `roster_2` uses `%m/%d/%Y` date format; others do not
       * `roster_5` has an outlying order of columns
2. **Attempt merge of data** (Refer to `eda.ipynb`)
    * Premise:
        * Parse `roster_4` state column into state names, not state abbreviations
        * Parse `roster_2` date columns into the format of `%Y-%m-%d`