import pandas as pd
import numpy as np
import sqlite3
import textwrap
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
from IPython.display import display
import re
from typing import Dict, List, Tuple, Optional, Literal
import argparse
import shutil

import warnings
warnings.filterwarnings("ignore") ## Suppress unnecessary warning prints

## Constants
STATE_MAPPER = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming'
}
KNOWN_PAYER = {
    "Mdcd", "Madv"
}

## Get data from SQLite3 Command 
READ_SQL_TO_PANDAS = lambda table_name: f"SELECT * FROM {table_name};"

## Printing Colors & Styles
BOLD = "\033[1m"
UNDERLINE = "\033[4m"
ENDC = "\033[0m" # Reset
# Text Color
BLACK = "\033[30m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"
BRIGHT_BLACK = "\033[90m"
BRIGHT_RED = "\033[91m"
BRIGHT_GREEN = "\033[92m"
BRIGHT_YELLOW = "\033[93m"
BRIGHT_BLUE = "\033[94m"
BRIGHT_MAGENTA = "\033[95m"
BRIGHT_CYAN = "\033[96m"
BRIGHT_WHITE = "\033[97m"
# Backgrounds
BG_RED = "\033[41m" 
BG_GREEN = "\033[42m" 
BG_BLUE = "\033[44m" 
BG_YELLOW = "\033[103m"
# Types
Theme = Literal[
    "BLACK", "RED", "GREEN", "YELLOW", "BLUE", "MAGENTA", "CYAN", "WHITE",
    "BRIGHT_BLACK", "BRIGHT_RED", "BRIGHT_GREEN", "BRIGHT_YELLOW", 
    "BRIGHT_BLUE", "BRIGHT_MAGENTA", "BRIGHT_CYAN", "BRIGHT_WHITE"
]
Background_Theme = Literal[
    "BG_RED", "BG_GREEN", "BG_BLUE", "BG_YELLOW"
]
# Mapping
STYLE_CODES: dict[Theme, str] = {
    "BLACK": BLACK,
    "RED": RED,
    "GREEN": GREEN,
    "YELLOW": YELLOW,
    "BLUE": BLUE,
    "MAGENTA": MAGENTA,
    "CYAN": CYAN,
    "WHITE": WHITE,
    "BRIGHT_BLACK": BRIGHT_BLACK,
    "BRIGHT_RED": BRIGHT_RED,
    "BRIGHT_GREEN": BRIGHT_GREEN,
    "BRIGHT_YELLOW": BRIGHT_YELLOW,
    "BRIGHT_BLUE": BRIGHT_BLUE,
    "BRIGHT_MAGENTA": BRIGHT_MAGENTA,
    "BRIGHT_CYAN": BRIGHT_CYAN,
    "BRIGHT_WHITE": BRIGHT_WHITE,
}
BACKGROUND_CODES: dict[Background_Theme, str] = {
    "BG_RED": BG_RED,
    "BG_GREEN": BG_GREEN,
    "BG_BLUE": BG_BLUE,
    "BG_YELLOW": BG_YELLOW
}
# Custom Print Function
def styled_log(message: str, 
               theme: Optional[Theme] = None, 
               bg_theme: Optional[Background_Theme] = None,
               bold: bool = False,
               underline: bool = False,
               level: Optional[Literal["info", "warning", "error"]] = None,
               end: str = "\n") -> None:
    
    if level == "error":
        theme = "BRIGHT_WHITE"
        bg_theme = "BG_RED"
        bold = True
    elif level == "warning":
        theme = "BRIGHT_WHITE"
        bg_theme = "BG_YELLOW"
        bold = True

    style = STYLE_CODES.get(theme, "")
    background = BACKGROUND_CODES.get(bg_theme, "")
    prefix = ""
    if bold:
        prefix += "\033[1m"
    if underline:
        prefix += "\033[4m"
    
    print(f"{prefix}{style}{background}{message}{ENDC}", end=end)

def print_dataframe_preview(df: pd.DataFrame, rows: int = 5, max_col_width: int = 20):
    """
    Print a clean preview of a DataFrame in the terminal.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to display
    rows : int
        Number of rows to show
    max_col_width : int
        Maximum width per column before truncation
    """
    term_width = shutil.get_terminal_size((100, 20)).columns
    num_cols = len(df.columns)
    col_width = max(min(term_width // num_cols, max_col_width), 8)

    def format_cell(val):
        val = str(val)
        return val if len(val) <= col_width else val[:col_width - 3] + "..."

    preview_df = df.head(rows).copy()
    for col in preview_df.columns:
        preview_df[col] = preview_df[col].map(format_cell)

    print(preview_df.to_string(index=False))

def read_database(path_to_db: str, verbose: bool = False, 
                  theme: Optional[Theme] = None, bg_theme: Optional[Background_Theme] = None
                  ) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    """
    Opens a connection to a SQLite database and returns a cursor object.

    Parameters
    ----------
    path_to_db : str
        The file path to the SQLite `.db` database file
    verbose : bool, optional
        Verbosity, by default False
    theme : Theme, optional
        Logging text color
    bg_theme : Background_Theme, optional 
        Logging background color

    Returns
    -------
    sqlite3.Cursor
        A cursor object used to execute SQL commands and interact with the database.
    """
    conn = sqlite3.connect(path_to_db)
    cur = conn.cursor()
    if verbose:
        styled_log(f"Connected to database {UNDERLINE + path_to_db}", theme=theme, bg_theme=bg_theme, bold=True)
    return conn, cur

def get_tables(
    cursor: sqlite3.Cursor, prefix: Optional[str] = None, verbose: bool = False, 
    theme: Optional[Theme] = None, bg_theme: Optional[Background_Theme] = None
    ) -> List[str]:
    """
    Get tables inside `.db` file - filterable by `prefix`

    Parameters
    ----------
    cursor : sqlite3.Cursor
        A cursor object used to execute SQL commands and interact with the database.
    prefix : Optional[str]
        Prefix desired tables start with.
    verbose : bool, optional
        Verbosity, by default False
    theme : Theme, optional
        Logging text color
    bg_theme : Background_Theme, optional 
        Logging background color
    """
    SQL_LIST_TABLE = """
        SELECT name FROM sqlite_master WHERE type = 'table';
    """
    tables: List[Tuple[str,]] = [tab for tab in cursor.execute(SQL_LIST_TABLE)]
    tables = [tab[0] for tab in tables]
    
    if prefix:
        tables = [tab for tab in tables if tab.startswith(prefix)]
        
    if verbose:
        if prefix:
            styled_log(
                f"Tables with prefix {prefix}: {tables}", 
                theme=theme, bg_theme=bg_theme
                )
        else:
            styled_log(
                f"Tables: {tables}", 
                theme=theme, bg_theme=bg_theme
                )
    
    return tables

def parse_date(
    df: pd.DataFrame, input_format: str = None,
    output_format: str = "%Y-%m-%d", error="coerce",   
    verbose: bool = False, inplace: bool = False,
    theme: Optional[Theme] = None, bg_theme: Optional[Background_Theme] = None, indent: int = 0
    ) -> Optional[pd.DataFrame]:
    """
    Detect date columns & parse them into `output_format`

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be processed
    input_format : str, optional
        Target format to be parsed - if None, parse all date formats
    output_format : str, optional
        Desired date format to be finalized - default: %Y-%m-%d
    error : str, optional
        Error parameter for pd.to_datetime() function
    verbose : bool, optional
        Verbosity, by default False
    theme : Theme, optional
        Logging text color
    bg_theme : Background_Theme, optional 
        Logging background color
    inplace : bool, optional
        Inplace modification - if False, return the modified dataframe

    Returns
    -------
    Optional[pd.DataFrame]
        Dataframe with date parsed to the correct format 
            (if inplace modification is not enabled)
    """
    
    modified_data = df if inplace else df.copy()
    
    for col in modified_data.columns:
        try:
            parser = pd.to_datetime(modified_data[col], format=input_format, errors=error)

            is_datetime = pd.api.types.is_datetime64_any_dtype(parser)
            parsed_count = parser.notna().sum()

            if is_datetime and parsed_count > 0:
                modified_data[col] = parser.dt.strftime(output_format)
                if verbose:
                    styled_log(f"{'\t'*indent}Column {col} [date parsing] - Status: Parsed",
                               theme=theme, bg_theme=bg_theme)

        except Exception as e:
            if verbose:
                styled_log(f"{'\t'*indent}Column {col} [date parsing] - Status: Failed",
                           theme=theme, bg_theme=bg_theme)
            pass
        
    return modified_data if not inplace else None

def parse_state(
    data: pd.Series, verbose: bool = False,
    theme: Optional[Theme] = None, bg_theme: Optional[Background_Theme] = None,
    indent: int = 0, inplace: bool = False
) -> pd.Series:
    """
    Parse `State` column into full state name

    Parameters
    ----------
    data : pd.Series
        Data series of state values
    verbose : bool, optional
        Verbosity, by default False
    theme : Theme, optional
        Logging text color
    bg_theme : Background_Theme, optional 
        Logging background color

    Returns
    -------
    pd.Series
        Processed state series
    """
    modified_state = data if inplace else data.copy()
    
    ## Full name case
    lower_values = {v.lower() for v in STATE_MAPPER.values()}
    if modified_state.dropna().apply(lambda x: isinstance(x, str) and x.lower() in lower_values).all():
        if verbose:
            styled_log(f"{'\t' * indent}Column with state values [state parsing] - Status: Skipped, already full name",
                       theme=theme, bg_theme=bg_theme)
        return data if inplace else modified_state
    
    try:
        modified_state = modified_state.map(lambda s: STATE_MAPPER[s])
        if verbose:
            styled_log(f"{indent*'\t'}Column with state values [state parsing] - Status: Parsed", theme=theme, bg_theme=bg_theme)
    except Exception as e:
        if verbose:
            styled_log(f"{indent*'\t'}Column with state values [state parsing] - Status: Failed", theme=theme, bg_theme=bg_theme)
    
    return modified_state if not inplace else None

def validate_data(
    df: pd.DataFrame, df_title: Optional[str] = "UNKNOWN", verbose: bool = False
    ) -> bool:
    """
    Validate all columns in df

    Parameters
    ----------
    df_title: str, optional
        DataFrame name, "UNKNOWN" if not provided
    df : pd.DataFrame
        Data to be validated
    verbose : bool
        Verbosity, by default False

    Returns
    -------
    bool
        Indicator for data - valid(True)/invalid(False)
         
    Notes
    -----
    `df` should contain the following columns:
        * `Person_Id` - only contain numbers
        * `First_Name` - only contain alphabets, "-", "'", and space
        * `Last_Name` - only contain alphabets, "-", "'", and space
        * `Dob` - only contain valid dates
        * `Age` - only contain numbers
        * `Gender` - "male/female" as the only option in existing data
        * `Street_Address` - no hard enforcement implemented
        * `State` - only full state name or abbreviations
        * `City` - only alphabets, "-", "'" and space AND no leading space
        * `Zip` - only contain numbers
        * `eligibility_start_date` - only contain valid dates
        * `eligibility_end_date` - only contain valid dates
        * `payer` - "Mdcd/Madv", other payer accepted but warning will be triggered
    """
    
    def isConvertibleToDate(data: pd.Series) -> bool:
        """Check if series consist only of date data"""
        converted = pd.to_datetime(data, errors='coerce')
        return converted.notna().all() 
        
    def isMatchingFormat(data: pd.Series, date_format: str = "%Y-%m-%d") -> bool:
        """Return True if all non-null values in series match the exact datetime format"""
        try:
            pd.to_datetime(data.dropna(), format=date_format)
            return True
        except Exception:
            return False
    
    ## Member ID Check
    all_member_id_isdigit = df["Person_Id"].apply(lambda x: isinstance(x, str) and x.isdigit() and len(x) == 8).all() 
    
    ## Member First / Last Name Checks
    regex_pat = re.compile(r"^[A-Za-z]+([ .'\-][A-Za-z]+)*$", re.UNICODE) ## no number, limit punctuations to ["-", "'"", " "]
    def is_valid_name(name): 
        return isinstance(name, str) and bool(regex_pat.match(name))
    all_fname_valid = df["First_Name"].apply(is_valid_name).all()
    all_lname_valid = df["Last_Name"].apply(is_valid_name).all()
    
    ## Dob Checks
    all_dob_valid = isConvertibleToDate(df["Dob"])
    dob_matching_format = isMatchingFormat(df["Dob"])
    
    ## Age Checks
    all_age_isdigit = df["Age"].apply(lambda x: isinstance(x, str) and x.isdigit()).all()
    
    ## Gender Checks
    all_gender_valid = df["Gender"].apply(lambda x: x == "Male" or x == "Female").all()
    
    ## State Checks
    valid_states_lower = set(k.lower() for k in STATE_MAPPER.keys()) | set(v.lower() for v in STATE_MAPPER.values())
    all_state_valid = df["State"].apply(lambda x: isinstance(x, str) and x.lower() in valid_states_lower).all()
    
    ## City Checks
    all_city_valid = df["City"].apply(is_valid_name).all()
    
    ## Zip Checks
    all_zip_valid = df["Zip"].apply(lambda x: isinstance(x, str) and len(x) == 5 and x.isdigit()).all()
    
    ## Eligibility Date Checks
    all_eligibility_start_valid = isConvertibleToDate(df["eligibility_start_date"])
    start_date_matching_format = isMatchingFormat(df["eligibility_start_date"])
    all_eligibility_end_valid = isConvertibleToDate(df["eligibility_end_date"])
    end_date_matching_format = isMatchingFormat(df["eligibility_end_date"])
    
    ## Payer (Only warning)
    unexpected_payer = set(df["payer"].unique()) - KNOWN_PAYER
    
    ## Report issues 
    if verbose:
        styled_log(f"\t=== Table {df_title} Validation ===", theme="CYAN", bold=True)

        styled_log(
            f"\t\tAll member ids (`Person_Id`): {'Valid' if all_member_id_isdigit else 'Invalid - - - ERROR'}",
            level="error" if not all_member_id_isdigit else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll first names (`First_Name`): {'Valid' if all_fname_valid else 'Invalid - - - ERROR'}",
            level="error" if not all_fname_valid else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll last names (`Last_Name`): {'Valid' if all_lname_valid else 'Invalid - - - ERROR'}",
            level="error" if not all_lname_valid else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll dates of birth (`Dob`): "
            f"{'Valid' if all_dob_valid else 'Invalid - - - ERROR'} | "
            f"Format: {'MATCH' if dob_matching_format else 'NOT MATCH'}",
            level="error" if not all_dob_valid else "warning" if not dob_matching_format else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll ages (`Age`): {'Valid' if all_age_isdigit else 'Invalid - - - ERROR'}",
            level="error" if not all_age_isdigit else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll genders (`Gender`): {'Valid' if all_gender_valid else 'Invalid - - - ERROR'}",
            level="error" if not all_gender_valid else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll states (`State`): {'Valid' if all_state_valid else 'Invalid - - - ERROR'}",
            level="error" if not all_state_valid else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll cities (`City`): {'Valid' if all_city_valid else 'Invalid - - - ERROR'}",
            level="error" if not all_city_valid else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll zip codes (`Zip`): {'Valid' if all_zip_valid else 'Invalid - - - ERROR'}",
            level="error" if not all_zip_valid else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll eligibility starting dates (`eligibility_start_date`): "
            f"{'Valid' if all_eligibility_start_valid else 'Invalid - - - ERROR'} | "
            f"Format: {'MATCH' if start_date_matching_format else 'NOT MATCH'}",
            level="error" if not all_eligibility_start_valid else "warning" if not start_date_matching_format else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\tAll eligibility ending dates (`eligibility_end_date`): "
            f"{'Valid' if all_eligibility_end_valid else 'Invalid - - - ERROR'} | "
            f"Format: {'MATCH' if end_date_matching_format else 'NOT MATCH'}",
            level="error" if not all_eligibility_end_valid else "warning" if not end_date_matching_format else None,
            theme="CYAN"
        )

        styled_log(
            f"\t\t{len(unexpected_payer)} unexpected payer type(s) found: "
            f"{'None' if len(unexpected_payer) == 0 else str(unexpected_payer) + ' - - - WARNING'}",
            level="warning" if len(unexpected_payer) > 0 else None,
            theme="CYAN"
        )
    
    return all_member_id_isdigit and all_fname_valid and all_lname_valid and all_dob_valid and \
            all_age_isdigit and all_gender_valid and all_state_valid and all_city_valid and \
                all_zip_valid and all_eligibility_start_valid and all_eligibility_end_valid
    
def parse_data(data: pd.DataFrame, df_title: str = "UNKNOWN", state_col_name: str = "State", verbose: bool = False) -> pd.DataFrame:
    """
    Function to parse the complete table.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame requiring parsing
    state_col_name : str, optional
        Column consisting state variables, by default "State"
    verbose : bool, optional
        Verbosity, by default False

    Returns
    -------
    pd.DataFrame
        Parsed DataFrame
        Columns: `member_id`, `member_first_name`, `member_last_name`, `date_of_birth`, 
                `main_address`, `state`, `city`, `zip_code`, `eligibility_start_date`, 
                `eligibility_end_date`, `payer`
    """
    
    if verbose:
            styled_log(
                f"\t===Parsing Table===",
                theme="BRIGHT_BLUE",
                bold=True
                )
    ### Parse Data
    data = parse_date(data, verbose=verbose, theme="BRIGHT_BLUE", indent=2)
    ### Parse State
    data[state_col_name] = parse_state(data=data[state_col_name], verbose=verbose, theme="BRIGHT_BLUE", indent=2)
    
    return data

def write_to_db(table_name: str, data: pd.DataFrame, conn: sqlite3.Connection, cursor: sqlite3.Cursor, overwrite: bool = False,
                 verbose: bool = False, theme: Optional[Theme] = None,
                 bg_theme: Optional[Theme] = None) -> pd.DataFrame:
    """
    Create table in .db file

    Parameters
    ----------
    table_name : str
        Desired Table Name, if exists, do union-write
    data : pd.DataFrame
        Data to be written in .db
    conn : sqlite3.Connection
        Connection to database
    cur : sqlite3.Cursor
        Cursor object for SQL operation
    overwrite : bool
        Wipe past data and insert the new (or not)
    verbose : bool, optional
        Verbosity, by default False
    theme : Theme, optional
        Logging text color
    bg_theme : Background_Theme, optional 
        Logging background color
        
    Returns
    -------
    pd.DataFrame
        Table snapshot of `std_member_info`
        
    Notes
    -----
    * `data` is assumed to be validated and parsed
    """
    
    ## DB access
    tables = get_tables(cursor)
    table_exists = table_name in tables

    ## Get existing data
    existing_data = pd.DataFrame()
    if table_exists:
        existing_data = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        if verbose:
            styled_log(f"Data exists in table '{table_name}' — reading existing data...", level="warning", theme=theme)
        existing_data = parse_date(existing_data, verbose=verbose, theme=theme, bg_theme=bg_theme, indent=1)

    existing_count = len(existing_data)
    new_count = len(data)

    if verbose:
        styled_log(f"Existing rows: {existing_count}", theme=theme)
        styled_log(f"New rows to add: {new_count}", theme=theme)

    ## Combine data - depending on overwrite
    if overwrite or not table_exists:
        combined_data = data
        if verbose and overwrite:
            styled_log(f"Overwriting table '{table_name}' with new data...", level="warning", theme=theme)
    else:
        combined_data = pd.concat([existing_data, data], ignore_index=True)
    
    combined_data = parse_date(df=combined_data, theme=theme, bg_theme=bg_theme)
    combined_data = combined_data.drop_duplicates() ## unique set

    final_count = len(combined_data)
    if overwrite or not table_exists:
        added_unique_rows = new_count
        removed_dupes = 0
    else:
        added_unique_rows = final_count - existing_count
        removed_dupes = new_count - added_unique_rows

    if verbose:
        styled_log(f"Duplicates removed from new data: {removed_dupes}", theme=theme)
        styled_log(f"Unique new rows added: {added_unique_rows}", theme=theme)
        styled_log(f"Final row count in table '{table_name}': {final_count}", theme=theme, bold=True)

    # Write to SQL
    combined_data.to_sql(table_name, conn, if_exists="replace", index=False)
    if verbose:
        styled_log(f"Data written to table `{table_name}`.", theme=theme, bg_theme=bg_theme, bold=True)
    
    return combined_data

def main(db_path: str, verbose: bool, overwrite: bool): 
    
    ## Establish Database Connection
    conn, cur = read_database(path_to_db=db_path)
    roster_data = pd.DataFrame()
    for tab in get_tables(cursor=cur, prefix="roster_"):
        ## READ SQL
        if verbose:
            styled_log(f"Processing table {tab}...",
                       theme="BRIGHT_WHITE", bg_theme="BG_BLUE", bold=True)
        tmp_data = pd.read_sql_query(READ_SQL_TO_PANDAS(tab), conn)
        
        ## Check NULL
        null_count = tmp_data.isnull().any(axis=1).sum()
        if null_count > 0 and verbose:
            styled_log(f"\t{null_count} row(s) with null values dropped before validation.", 
                    level="warning", theme="CYAN")

        tmp_data = tmp_data.dropna()
        
        ## Validate data
        is_valid = validate_data(df=tmp_data, df_title=tab, verbose=verbose)
        
        if not is_valid:
            if verbose:
                styled_log(f"Skipping table {tab} due to invalid data detected.",
                           theme="BRIGHT_BLACK", bg_theme="BG_YELLOW", bold=True)
            pass ## Skipping table
        
        ## Parsing
        parsed_tmp_data = parse_data(data=tmp_data, state_col_name="State", verbose=verbose)
        if verbose:
            print_dataframe_preview(parsed_tmp_data) ## Sample
        
        ## Aggregation
        roster_data = pd.concat([roster_data, parsed_tmp_data], ignore_index=True)
        if verbose:
            styled_log(f"Table {tab} processed and added to aggregation.", bold=True, theme="BRIGHT_WHITE", bg_theme="BG_GREEN")
        print("\n\n") ## Separate logging
    
    if verbose:
        styled_log(f"Aggregation Completed: all valid roster data parsed & included.", bold=True, theme="BRIGHT_WHITE", bg_theme="BG_GREEN")
    
    ## Drop unwanted columns & Rename columns 
    roster_data = roster_data.rename(columns={
        "Person_Id": "member_id", 
        "First_Name": "member_first_name",
        "Last_Name": "member_last_name",
        "Dob": "date_of_birth",
        "Zip": "zip_code",
        "City": "city",
        "State": "state",
        "Street_Address": "main_address",
    }).drop(columns=["Age", "Gender"])
    
    ## Original Record Count & Duplicate Count
    data_size = len(roster_data)
    duplicate_row_count = roster_data[roster_data.duplicated(keep=False)].drop_duplicates().shape[0]
    
    if verbose:
        styled_log(f"All record size: {data_size}", theme="BRIGHT_WHITE", bg_theme="BG_BLUE", bold=True)
        styled_log(f"Duplcated record count: {duplicate_row_count}", theme="BRIGHT_WHITE", bg_theme="BG_BLUE", bold=True)
    
    ## Drop duplicates
    roster_data = roster_data.drop_duplicates()
    if verbose:
        styled_log(f"Unique record count: {len(roster_data)}", theme="BRIGHT_WHITE", bg_theme="BG_BLUE", bold=True)
    
    ## Filter to 2025
    roster_data["eligibility_start_date"], roster_data["eligibility_end_date"] = (
        pd.to_datetime(roster_data["eligibility_start_date"]),
        pd.to_datetime(roster_data["eligibility_end_date"])
    ) ## Making sure date is type-ready for comparing

    start_2025 = pd.Timestamp("2025-01-01")
    end_2025 = pd.Timestamp("2025-12-31")

    def overlaps_2025(row):
        return (row["eligibility_start_date"] <= end_2025) and (row["eligibility_end_date"] >= start_2025)

    roster_data = roster_data[roster_data.apply(overlaps_2025, axis=1)]
    
    if verbose:
        styled_log(f"Only {len(roster_data)} members are eligible in 2025.", theme="BRIGHT_BLUE")
    
    
    ## Write to .db
    member_info_data = write_to_db(table_name="std_member_info", data=roster_data, conn=conn, cursor=cur, overwrite=overwrite, verbose=verbose,
                theme="CYAN")
    
    if verbose:
        print("\n\n")
        styled_log(f"{db_path} updated!", theme="BRIGHT_WHITE", bg_theme="BG_GREEN", bold=True)
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Parsing data from n1_data_ops_challenge.db & Writing to `std_member_info` table."
    )
    parser.add_argument(
        "-db", "--database",
        required=True,
        help="Path to .db file"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enabling verbosity for script debugging"
    )
    parser.add_argument(
        "-ow", "--overwrite",
        action="store_true",
        help="New data overwrites `std_member_info`"
    )
    
    args = parser.parse_args()
    db_path = args.database
    verbose = args.verbose
    overwrite = args.overwrite
    
    main(db_path=db_path, verbose=verbose, overwrite=overwrite)