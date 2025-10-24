# Insurance Member Data Pipeline

Dataset: Member Rosters of Insurances

## Goals
1. Deliver a data pipeline for **scalable ingestion of data**, considered case -> **biweekly updates of new data**
2. Report a **summary of datasets** (customized for PM use case) for each update
3. Propose scalable methods for **triggering of Python script whenever data is updated**

## Installation

### Files Required
Put the database file inside the root directory. Make sure the name is `n1_data_ops_challenge.db`. 

**Important** Download `tl_2020_us_zcta520.zip` from [Census](https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/). Unzip the downloaded file at root of this repo.

### Conda Environment Set Up \[Optional\]
1. Create a **Conda** environment using the YAML configuration file
```conda env create -f environment.yml```
2. Configure kernel for Jupyter (in case of notebook revisits)
```python3 -m ipykernel install --user --name member-pipeline --display-name "Python 3 (member-pipeline)"```
    * When opening jupyter notebook, change the runtime kernel to `Python 3 (member-pipeline)`

### Pip Set Up
Install all required Python packages by:
```pip install -r requirements.txt```

### Install US Map
Visit [https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/](Census) to install `tl_2020_us_zcta520.zip`. Unzip it and extract all file - place in root directory here.

### Notebook Usage
I used **jupyterlab** for all `.ipynb` files. Simply do `jupyter lab` at root directory. 

## Footprint
1. **Exploration of data** (Refer to `eda.ipynb`)
    * Imported data as Pandas DataFrame
    * Got meta-data for each roster dataset & model score dataset
    * Checked columns for each roster dataset - all schemas are consistent
    * Checked for Null values -> no null value found
    * Sampled the dataset for content discrepency
       * `roster_4` uses state abbreviations; others do not
       * `roster_2` uses `%m/%d/%Y` date format; others do not
       * `roster_5` has an outlying order of columns
    * Duplicate member records found 
       * Duplicate conditions are due to date format mismatch
       * Considered cases when same member has multiple eligibility period - no such case found
2. **Attempt merge of data** (Refer to `eda.ipynb`)
    * Premise:
        * Parse `roster_4` state column into state names, not state abbreviations
        * Parse `roster_2` date columns into the format of `%Y-%m-%d`
    
    NOTE: Check table addressing potential concerns over data format in `eda.ipynb`
3. **Filtering unique member data with eligibility in 2025**
    * Unified all date format by `%Y-%m-%d`
    * Removed duplicates through `.drop_duplicates()` in `pandas`
    * Added filtering mask to isolate member records with eligibilty in 2025
    * Stored the aggregated, cleaned member records as `member_info_data` in notebook runtime
4. **Writing aggregated member records to new database file**
    * Created a new table `std_member_info` and wrote `member_info_data` into the table through `.to_sql()` in `pandas`
5. **Insights for quick data reporting to PM**
    * [Premise] Insights are from the entire dataset, not limited to 2025
    * Filtered data by `eligibility_start_date` and `eligibility_end_date` - checking specifically for April 2025 by comparing dates
    * Utilized results from EDA (step 1) addressing duplicate member to report members included more than once
    * Broke down data by `Payer`:
        * Only 2 payer types found
        * Extracted ratio of member under the 2 payer types
        * Extract gender ratio, eligibility status by time, age and geographic distribution
    * Computed score averages and maximums, and extracted member records according to those scores

## Singular Data Pipeline --- `singular-ingestion.py`

### Description
`singular-ingestion.py` facilitates an one-time data processing pipeline - streamlining data from `n1_data_ops_challenge.db` to build a new table `std_member_info` that includes all roster data. Specifically, the script reads through all tables whose names start with `roster_`, validates and parses the data, and ultimately aggregates them in order to write into `std_member_info` in `n1_data_ops_challenge.db`. 

### Running it
```
python singular-ingestion.py -db n1_data_ops_challenge.db [-v]
```
There are 3 arguments for `singular-ingestion.py` - please do `python singular-ingestion.py -h` to see description on usage of those arguments. They include verbosity, referencing of `.db` file and enabling of overwriting existing data in `n1_data_ops_challenge.db`