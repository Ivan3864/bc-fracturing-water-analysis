
"""
BC FracFocus Water Use – Data Cleaning Script

- Loads raw FracFocus (BIL-183) data
- Handles encoding
- Cleans and aggregates ingredient-level rows to fracture events
- Converts coordinates (DMS or decimal) to decimal degrees for mapping
- Outputs clean CSV
"""

from pathlib import Path
import re
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DATA = BASE_DIR / "data" / "raw" / "Fracture Fluid Data.csv"
CLEAN_DATA = BASE_DIR / "data" / "cleaned" / "fracture_events_cleaned.csv"


def dms_to_decimal(val, is_lon=False):
    """
    Convert DMS strings like '56 18 46.72' or '121 39 08.07' to decimal degrees.
    Also accepts already-decimal numbers (returns as float).
    Longitude is forced negative (BC is west).
    """
    if pd.isna(val):
        return None

    s = str(val).strip()

    # If it's already a simple decimal number, use it
    try:
        f = float(s)
        # For BC, longitude should be negative
        if is_lon and f > 0:
            f = -f
        return f
    except ValueError:
        pass

    # Extract DMS parts (space or colon separated)
    parts = re.split(r"[^\d.]+", s)
    parts = [p for p in parts if p != ""]
    if len(parts) < 3:
        return None

    deg, minutes, seconds = map(float, parts[:3])
    dec = deg + minutes / 60 + seconds / 3600

    if is_lon and dec > 0:
        dec = -dec

    return dec


# -----------------------------
# Load raw data
# -----------------------------
df = pd.read_csv(RAW_DATA, encoding="latin1", low_memory=False)

# -----------------------------
# Select relevant columns
# -----------------------------
cols_keep = [
    "Fracture Date",
    "UWI",
    "Well Area Name",
    "Latitude",
    "Longitude",
    "Total Water Volume (m^3)"
]
df = df[cols_keep].copy()

# -----------------------------
# Clean data types
# -----------------------------
df["Fracture Date"] = pd.to_datetime(df["Fracture Date"], format="%d-%b-%Y", errors="coerce")
df["Total Water Volume (m^3)"] = pd.to_numeric(df["Total Water Volume (m^3)"], errors="coerce")

# Convert coordinates to decimal degrees
df["Latitude_dd"] = df["Latitude"].apply(lambda x: dms_to_decimal(x, is_lon=False))
df["Longitude_dd"] = df["Longitude"].apply(lambda x: dms_to_decimal(x, is_lon=True))

# -----------------------------
# Remove invalid rows (but keep it realistic)
# -----------------------------
df = df.dropna(subset=["Fracture Date", "UWI", "Total Water Volume (m^3)"])
df = df.dropna(subset=["Latitude_dd", "Longitude_dd"])

# -----------------------------
# Aggregate to fracture events (dedupe ingredient rows)
# -----------------------------
df_events = (
    df.drop_duplicates(subset=["UWI", "Fracture Date"])
      .reset_index(drop=True)
)

# Add year for trends
df_events["Year"] = df_events["Fracture Date"].dt.year

# Keep only map-ready columns (clean output)
df_events_out = df_events[[
    "Fracture Date", "Year", "UWI", "Well Area Name",
    "Latitude_dd", "Longitude_dd", "Total Water Volume (m^3)"
]].copy()

# Save
df_events_out.to_csv(CLEAN_DATA, index=False, encoding="utf-8")

print("Data cleaning complete.")
print(f"Clean file written to: {CLEAN_DATA}")
print(f"Fracture events: {len(df_events_out)}")
print(df_events_out.head(3))
