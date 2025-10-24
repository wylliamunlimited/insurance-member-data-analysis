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
    
test_cases = [
    # Ground Truth (valid)
    ("ground_truth", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                      "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                      "2021-08-01", "2021-11-01", "Madv")),

    # Flawed Member ID
    ("flawed_member_id_1", ("12ab12", "John", "Doe", "11-22-2001", "23", "Male",
                            "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                            "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_member_id_2", ("12ab1212", "John", "Koe", "11-22-2001", "23", "Male",
                            "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                            "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_member_id_3", ("121212", "John", "Koe", "11-22-2001", "23", "Male",
                            "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                            "2021-08-01", "2021-11-01", "Madv")),

    # Flawed First Name
    ("flawed_fname_1", ("00000000", "John3", "Koe", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_fname_2", ("00000000", "John!", "Koe", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_fname_3", ("00000000", "St.John", "Koe", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),

    # Flawed Last Name
    ("flawed_lname_1", ("00000000", "John", "Leary!", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_lname_2", ("00000000", "John", "Leary3", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_lname_3", ("00000000", "John", "O'Leary", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),

    # Flawed State
    ("flawed_state_1", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "Cali", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_state_2", ("00000000", "John!", "Koe", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "california", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_state_3", ("00000000", "John!", "Koe", "11-22-2001", "23", "Male",
                        "1505 Alvarez Spur Suite 90", "ca", "Lake Sharonburgh", "93546",
                        "2021-08-01", "2021-11-01", "Madv")),

    # Flawed City
    ("flawed_city_1", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                       "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh 3", "93546",
                       "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_city_2", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                       "1505 Alvarez Spur Suite 90", "California", " Lake Sharonburgh", "93546",
                       "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_city_3", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                       "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh!", "93546",
                       "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_city_3", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                       "1505 Alvarez Spur Suite 90", "California", "St. Lake Sharonburgh", "93546",
                       "2021-08-01", "2021-11-01", "Madv")),

    # Flawed DOB
    ("flawed_dob_1", ("00000000", "John", "Koe", "No Date", "23", "Male",
                      "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                      "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_dob_2", ("00000000", "John", "Koe", "11-27", "23", "Male",
                      "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                      "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_dob_3", ("00000000", "John", "Koe", "29 August 2021", "23", "Male",
                      "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                      "2021-08-01", "2021-11-01", "Madv")),

    # Flawed Gender
    ("flawed_gender_1", ("00000000", "John", "Koe", "11-22-2001", "23", "No Gender",
                         "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                         "2021-08-01", "2021-11-01", "Madv")),

    # Flawed Age
    ("flawed_age_1", ("00000000", "John", "Koe", "11-22-2001", "No Age", "Male",
                      "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "93546",
                      "2021-08-01", "2021-11-01", "Madv")),

    # Flawed Zip
    ("flawed_zip_1", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                      "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "935461",
                      "2021-08-01", "2021-11-01", "Madv")),
    ("flawed_zip_2", ("00000000", "John", "Koe", "11-22-2001", "23", "Male",
                      "1505 Alvarez Spur Suite 90", "California", "Lake Sharonburgh", "abc12",
                      "2021-08-01", "2021-11-01", "Madv")),
]

def run_validation_tests(test_cases: List[Tuple[str, Tuple]], verbose: bool = True) -> None:
    """
    Run all test cases through validate_data and print results.

    Parameters
    ----------
    test_cases : List[Tuple[str, Tuple]]
        A list of (test_case_name, row_data) tuples.
    verbose : bool
        If True, each case is validated with detailed logging.
    """

    columns = [
        "Person_Id", "First_Name", "Last_Name", "Dob", "Age", "Gender", "Street_Address",
        "State", "City", "Zip", "eligibility_start_date", "eligibility_end_date", "payer"
    ]

    summary = []

    styled_log("Running Validation Test Suite", theme="MAGENTA", bold=True, underline=True)

    for case_name, row_data in test_cases:
        styled_log(f"\n--- Test Case: {case_name} ---", theme="BRIGHT_BLUE", bold=True)

        df = pd.DataFrame([row_data], columns=columns)

        result = validate_data(df, df_title=case_name, verbose=verbose)
        summary.append((case_name, result))

    styled_log("\n=== SUMMARY ===", theme="CYAN", bold=True)
    for case_name, passed in summary:
        styled_log(
            f"{case_name}: {'PASS' if passed else 'FAIL'}",
            level="error" if not passed else None,
            theme="GREEN" if passed else "RED"
        )

    return summary

run_validation_tests(test_cases=test_cases)