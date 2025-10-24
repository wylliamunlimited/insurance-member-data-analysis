# #
# # DRAFT OBSERVER SCRIPT FOR METHOD 3 (HAVE NOT BEEN TESTED)
# #

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent, FileCreatedEvent

import argparse
from pathlib import Path
import sys
import logging
import subprocess
import os
import time
import sqlite3
from typing import Literal, Optional
## \u2718 is cross

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

class FileHandler(FileSystemEventHandler):
    
    def __init__(self, pipeline_script: Path, db: Path, source: Path, fail_bin: Path, _bin: Path, verbose: bool):
        super().__init__()
        self.SUPPORTED_EXTENSION = ("csv", "txt", "json")
        self.pipeline_script = pipeline_script
        self.source = source
        self.fail_bin = fail_bin
        self.db = db
        self._bin = _bin
        self.verbose = verbose
        
        self.last_processed = None
    
    def on_created(self, event: FileSystemEvent) -> None:
        file_path = Path(event.src_path)
        
        ## Directory Handling - Recursive File Scan
        if event.is_directory:
            for file in Path(event.src_path).rglob("*"):
                if file.is_file():
                    self.on_created(FileCreatedEvent(str(file)))
            return
        
        ## Check extension
        ext = file_path.suffix.lstrip(".").lower()
        if ext not in self.SUPPORTED_EXTENSION:
            styled_log((f"\u27B3 File type .{ext} not supported in data "
                                 f"ingestion pipeline - please reformat. Skipping {file_path.name}..."), 
                       theme="WHITE", bg_theme="BG_BLUE")
            
            ## Moving it to fail bin
            self._transfer_failed_file(file_path=self.fail_bin, reason=f"Unsupported file type: {ext}")
            
            return
            
        ## File is good to go
        styled_log(f"\u2714 New file detected: {event.src_path}", 
                   theme="WHITE", bg_theme="BG_BLUE")
        time.sleep(1)
        
        try: 
            
            command = [
                "python", str(self.pipeline_script),
                "--database", str(self.db),
                "--source", str(file_path),
                "--bin", str(self._bin),
                "--failbin", str(self.fail_bin)
            ]
            if self.verbose:
                command += ["--verbose"]
                styled_log(f"\u2708 Running command: {' '.join(command)}", theme="WHITE", bg_theme="BG_BLUE")
            
            subprocess.run(command, check=True, stderr=sys.stderr, stdout=sys.stdout)
            styled_log(f"\u2714 Successfully processed: {file_path.name}", theme="WHITE", bg_theme="BG_BLUE")
            
        except Exception as e:
            styled_log(f"\u27B3 Failed to process {file_path.name}: {e}", level="warning")
            
    def on_modified(self, event):
        file_path = Path(event.src_path)
        
        if file_path.suffix != ".db" or file_path.name != self.db.name or event.is_directory:
            return
        
        now = time.time()
        last = self.last_processed
        
        if not last or now - last < 10:
            return ## skip if it is within 10 seconds
        
        self.last_processed = now
        
        try:
            command = [
                "python", str(self.pipeline_script),
                "--database", str(self.db),
                "--source", str(file_path),
                "--bin", str(self._bin),
                "--failbin", str(self.fail_bin)
            ]
            if self.verbose:
                command += ["--verbose"]
                styled_log(f"\u2708 Running command: {' '.join(command)}", theme="WHITE", bg_theme="BG_BLUE")
            
            subprocess.run(command, check=True, stderr=sys.stderr, stdout=sys.stdout)
            styled_log(f"\u2714 Successfully processed: {file_path.name}", theme="WHITE", bg_theme="BG_BLUE")
            
        except Exception as e:
            styled_log(f"\u27B3 Failed to process {file_path.name}: {e}", level="warning")
            
    
    def _transfer_failed_file(self, file_path: Path, reason: str):
        target_path = self.fail_bin / file_path.name
        
        try: 
            file_path.rename(target=target_path)
            styled_log(f"\u27B3 {file_path.name} moved to {self.fail_bin} - Reason: {reason}", level="warning")
        except Exception as e:
            styled_log(f"\u2718 Failed to move {file_path.name} to {self.fail_bin}: {e}", level="error")
            
            
if __name__ == "__main__":
    
    # CLI Arguments
    parser = argparse.ArgumentParser(
        description="Watch a directory for new file creation to prepare for ingestion."
    )
    parser.add_argument(
        "-p", "--pipeline",
        required=True,
        help="Script for data ingestion pipeline."
    )
    parser.add_argument(
        "-s", "--source",
        required=True,
        help="Directory to monitor for new data files."
    )
    parser.add_argument(
        "-b", "--bin",
        required=True,
        help="Directory processed files will be placed in."
    )
    parser.add_argument(
        "-f", "--failbin",
        required=True,
        help="Directory where files that failed ingestion go to."
    )
    parser.add_argument(
        "-db", "--database",
        required=True,
        help="Database hosting processed data."
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output and logging."
    )
    args = parser.parse_args()

    
    
    ## Source & Bin directory args
    source, _bin, fail_bin, db = Path(args.source).expanduser(), Path(args.bin).expanduser(), Path(args.failbin).expanduser(), Path(args.db).expanduser()
    if not _bin.is_absolute():
        _bin = Path.cwd() / _bin
    if not source.is_absolute():
        source = Path.cwd() / source
    if not fail_bin.is_absolute():
        fail_bin = Path.cwd() / fail_bin
    if not db.is_absolute():
        db = Path.cwd() / db

    if not db.exists():
        styled_log("No DB found.", level="error")
        sys.exit(1)
        
    ## Make directories if not exists
    source.mkdir(parents=True, exist_ok=True)
    _bin.mkdir(parents=True, exist_ok=True)
    fail_bin.mkdir(parents=True, exist_ok=True)
    
    
    ## Ingestion Pipeline Script
    script_path = Path(args.pipeline).expanduser().resolve()
    if not script_path.exists():
        styled_log(f"\u2718 Pipeline script not found at: {script_path.suffix}", level="error")
        sys.exit(1)
    
    if script_path.suffix != ".py":
        styled_log(f"\u2718 Pipeline script must be a Python (.py) file. Got: {script_path.suffix}", level="error")
    
    
    ## Watchdog
    event_handler = FileHandler(
        pipeline_script=script_path,
        source=source,
        fail_bin=fail_bin,
        _bin=_bin,
        db=db,
        verbose=args.verbose
    )
    observer = Observer()
    observer.schedule(event_handler=event_handler, path=str(source.resolve()), recursive=False)
    
    observer.start()
    
    ## Heading summary    
    styled_log(f"\u2730\u2730\u2730 Monitoring: {source.resolve()}", theme="CYAN")
    styled_log(f"\u2730\u2730\u2730 Dumping processed data into: {_bin.resolve()}", theme="CYAN")
    styled_log(f"\u2730\u2730\u2730 Dumping failed data into: {fail_bin.resolve()}", theme="CYAN")
    styled_log(f"\u2730\u2730\u2730 Listening...", theme="CYAN")

    ## Endless Loop
    try: 
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()