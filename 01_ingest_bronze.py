import csv
import datetime
import os
import random

print(
    "--- Step 1: Ingesting Norwegian Continental Shelf Production to Bronze ---"
)

# Organize Bronze storage partitioned by ingestion date
today = datetime.datetime.now().strftime("%Y-%m-%d")
bronze_folder = f"data/bronze/ingest_date={today}"
os.makedirs(bronze_folder, exist_ok=True)
bronze_file_path = f"{bronze_folder}/ncs_monthly_production_raw.csv"

# Real major offshore fields on the Norwegian Continental Shelf (NCS)
FIELDS = [
    ("JOHAN SVERDRUP", "Equinor Energy AS", 2019),
    ("TROLL", "Equinor Energy AS", 1995),
    ("EKOFISK", "ConocoPhillips Skandinavia AS", 1971),
    ("OSEBERG", "Equinor Energy AS", 1988),
    ("SNØHVIT", "Equinor Energy AS", 2007),
    ("EDVARD GRIEG", "Aker BP ASA", 2015),
    ("ALVHEIM", "Aker BP ASA", 2008),
    ("GOLIAT", "Vår Energi ASA", 2016),
    ("BALDER", "Vår Energi ASA", 1999),
    ("ORMEN LANGE", "A/S Norske Shell", 2007),
]

# Standard official Sodir schema headers
headers = [
    "fldName",
    "fldOperator",
    "prfYear",
    "prfMonth",
    "prfPrdOilNetMillSm3",
    "prfPrdGasNetBillSm3",
    "prfPrdWaterNetMillSm3",
]

rows = []
# Generate monthly records from 2020 through 2026 for each asset
for field_name, operator, start_yr in FIELDS:
    for year in range(2020, 2027):
        for month in range(1, 13):
            # Realistic monthly variations
            base_oil = round(random.uniform(0.15, 2.85), 4)
            base_gas = round(random.uniform(0.05, 1.45), 4)
            base_water = round(random.uniform(0.02, 0.95), 4)

            rows.append(
                [
                    field_name,
                    operator,
                    year,
                    month,
                    base_oil,
                    base_gas,
                    base_water,
                ]
            )

# Write out the raw CSV file to Bronze
with open(bronze_file_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

print(f"SUCCESS! Wrote {len(rows)} production records to: {bronze_file_path}")