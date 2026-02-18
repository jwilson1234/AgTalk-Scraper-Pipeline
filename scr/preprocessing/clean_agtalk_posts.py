"""
Text Cleaning Pipeline for scraped AgTalk Data

Description:
    Cleans raw scraped post text for NLP modeling

Cleans:
    - Fixes encoding artifacts
    - Removes dates and timestamps from text
    - Removes URLs from text
    - Removes attatchment references from text
    - Normalizes whitespace
    - Removes short posts < 20 characters

"""

# Standard Library Imports

import re

# Third-Party Imports

import pandas as pd

# File Paths

INPUT_PATH = "data/raw/precision_talk_2021_2026.csv"

OUTPUT_PATH = "data/processed/precision_talk_five_year_cleaned.csv"



# Raw Data Cleaning Function

def clean_text_column(series: pd.Series) -> pd.Series:
    
    """
    Applies text normalization and data cleaning to raw post text
    """

    
    # Ensuring text type

    series = series.astype(str)

    # Fixing encoding artifacts

    series = series.apply(
        lambda x: x.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
    )

    # Removing dates and timestamps from text

    series = series.str.replace(
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\s*\d{0,2}:?\d{0,2}", "", regex=True
    )
    series = series.str.replace(
        r"\b\d{1,2}:\d{2}\b", "", regex=True
    )

    # Removing URLs from text

    series = series.str.replace(
        r"http\S+|www\S+", "", regex=True
    )

    # Removing common metadata artifacts from text

    series = series.str.replace(r"Edited by .*", "", regex=True)
    series = series.str.replace(r"Attachments.*", "", regex=True)

    # Removing image/file references from text

    series = series.str.replace(r"\S+\.(jpg|jpeg|png|pdf)", "", regex=True)
    series = series.str.replace(r"\d+KB\s*-\s*\d+\s*downloads", "", regex=True)
    series = series.str.replace(r"\(.*?full\)\.", "", regex=True)

    # Fixing known encoding artifact

    series = series.str.replace("‚Äô", "'", regex=False)

    # Cleaning spacing artifacts

    series = series.str.replace(r"\s+\)", ")", regex=True)
    series = series.str.replace(r"\(\s+", "(", regex=True)

    # Normalizing whitespace

    series = series.str.replace(r"\s+", " ", regex=True).str.strip()

    return series


# main block

if __name__ == "__main__":
    df = pd.read_csv(INPUT_PATH)

    df["clean_text"] = clean_text_column(df["post_text"])

    # Remove short posts
    
    df = df[df["clean_text"].str.len() >= 20]

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
