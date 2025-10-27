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
There are 3 arguments for `singular-ingestion.py` - please do `python singular-ingestion.py -h` to see description on usage of those arguments. They include verbosity, referencing of `.db` file and enabling of overwriting existing data in `n1_data_ops_challenge.db`.

### Scaling
This script itself is ready for new data ingestion - tradeoff is we have to set an alarm and run it ourselves every 2 weeks. To automate the biweekly update of data, we need an automated method that 1. detects data influx activity and 2. triggers the ingestion pipeline accordingly. 

And since data influx could be unpredictable, we also need methods to account for not only `.db` new table updates, but also other medium of data influx, such as other file formats. This is for much higher convenience and flexibility over data flow. Sometimes whoever updates the data might not know SQL and could directly insert new data into the `.db` file. We can picture in scenarios we have entries of data through manual spreadsheet input or file upload over file formats that are more convenient for frontend developers.

## Automated Data Ingestion Pipeline Design
There are many ways we could consider for the data ingestion to be automated with robust design. Specifically, I thought through 3 methods for streamlining data with CI/CD ingestion pipeline. 

### Pub/Sub + Storage Bucket Through Cloud (Supporting non-SQL Data Update)

The first method involves using cloud services, specifically **Pub/Sub** and **cloud storage**. The architecture includes setting up a **subscriber** server that hosts the database and its update functionality (where Python script and `.db` reside) upon new data activity, especially creation. We will also setup a middleman server (the distributor) that distributes the messages. We will utilize the backend server of our platform (assuming that is how N1 Health access new data) as the **publisher**.

For example, when new data is uploaded through the frontend software UI (say drag-and-drop file) and the backend server of the platform was notified the upload, it would first transfer file to a cloud storage while publishing a message that new data activity was recorded. Then our distributor host would notify subscribers for the change. (Notice here I am referencing multiple subscribers - good for speedups and parallelization of data pipeline) Then the subscribers (data pipeline hosts) would trigger the Python script that 
1. takes the `.db` and data file from cloud storage, 
2. conduct data ingestion pipeline, 
3. update the `.db` file and 
4. upload the updated version of `.db` back to cloud storage (that way the virtual machine hardware requirements would not be overwhelmed and thus cheaper in cost)

#### Benefit
* Automated process - new data are detected and processing happens automatically
* Bulk data handling - multiple subscribers act as load balancers to speed up data handling


### Cloud Function (Serverless) + Storage Bucket Through Cloud

Second method includes simply hosting a serverless Python script endpoint through cloud platform. We can simply put a Python script on cloud function services (e.g. Google Cloud Functions, AWS Lambda). They usually come with sdk library by cloud providers that enables new file upload within cloud storage. 

When a new file is uploaded, cloud reports the activity to the function endpoint and therefore triggers it - very intuitive. 

#### Benefit
* Automated process - new data are detected and processing happens automatically

#### Hard to scale 
Cloud functions are not built for computationally intense work - so bulk data processing would not be efficiently done through function service.

### Server Based Solution (Optional Cloud Usage)

We could also host a virtual machine on the cloud or other hosting services. This is demonstrated in the repository. We will use Python `Watchdog` library to detect file activity (any computer activity streaming toolkit can work).

Basically, we run a daemon script (program running by itself underthehood) to monitor updates within a targeted directory in the computer system (e.g. creation of file, modification of file). And once a creation of file is detected there, we run the pipeline to handle the new file immediately. 

### Example of Automated Processing
I drafted the implementation in `ingestion.py` and `guard.py`. `guard.py` is responsible for monitoring system changes (creation of `csv`, `txt`, `json` files OR modification of the database file). Upon changes, it would run the `ingestion.py` with the data in either files or new tables in database file. 

A special note on this method is that, the ingestion pipeline requires path to 3 directories (a data source directory, a processed file directory, and a directory for files that could not be processed). Any processed files (except `.db`) would be transfered to the processed directory specified by command arguments, and files that throw error in the pipeline would end up in failed directory. 

#### Proposed Running Methods
```python guard.py -p path/to/ingestion/script -s data/directory -b path/to/processed/directory -f path/to/failed/directory -db path/to/database/file [-v]```

It runs continuously until interrupted by keyboard termination. Every new data detection would trigger the following (not to be run manually):
```python ingestion.py -s path/to/data/file -b path/to/processed/directory -f path/to/failed/directory -db path/to/database/file [-v]```

The pipeline is implemented with the same step as `singular-ingestion.py` with conditional file handling.
