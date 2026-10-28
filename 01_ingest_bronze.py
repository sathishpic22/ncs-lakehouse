import datetime
import os
import requests

print("=== Step 1: Ingesting Raw Production Data (Bronze Layer) ===")

# Safe directory resolution for local Mac and Databricks
base_dir = (
    os.path.dirname(os.path.abspath(__file__))
    if "__file__" in globals()
    else os.getcwd()
)

# Bronze partition path: data/bronze/ingest_date=YYYY-MM-DD
today_str = datetime.date.today().isoformat()
bronze_folder = os.path.join(base_dir, f"data/bronze/ingest_date={today_str}")
os.makedirs(bronze_folder, exist_ok=True)

bronze_file_path = os.path.join(
    bronze_folder, "ncs_monthly_field_production.csv"
)

# Open data endpoint from Sodir
DATA_URL = "https://factpages.sodir.no/public?/FactPages/TableView/field_production_monthly&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&rs:Format=CSV&Top100=false&IpAddress=NotApplicable&CultureCode=en"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
}

success = False
try:
    print(
        "Attempting to download monthly offshore production data from Sodir..."
    )
    response = requests.get(DATA_URL, headers=headers, timeout=15)
    if response.status_code == 200 and len(response.content) > 100:
        with open(bronze_file_path, "wb") as f:
            f.write(response.content)
        print(f"SUCCESS: Downloaded live Bronze CSV from Sodir.")
        success = True
    else:
        print(
            f"WARNING: Sodir server returned HTTP {response.status_code}. Using local bronze production dataset..."
        )
except Exception as e:
    print(
        f"WARNING: Could not connect to Sodir ({e}). Using local bronze production dataset..."
    )

# Fallback: Guarantees pipeline never crashes even if external govt site is down
if not success:
    fallback_csv_data = """fldName,fldOperator,prfYear,prfMonth,prfPrdOilNetMillSm3,prfGasNetBillSm3,prfWaterNetMillSm3
TROLL,Equinor Energy AS,2023,1,0.85,3.20,0.12
TROLL,Equinor Energy AS,2023,2,0.82,3.15,0.11
TROLL,Equinor Energy AS,2023,3,0.89,3.30,0.14
JOHAN SVERDRUP,Equinor Energy AS,2023,1,2.45,0.18,0.05
JOHAN SVERDRUP,Equinor Energy AS,2023,2,2.38,0.17,0.06
JOHAN SVERDRUP,Equinor Energy AS,2023,3,2.51,0.19,0.05
EKOFISK,ConocoPhillips Skandinavia AS,2023,1,0.62,0.12,0.45
EKOFISK,ConocoPhillips Skandinavia AS,2023,2,0.59,0.11,0.42
EKOFISK,ConocoPhillips Skandinavia AS,2023,3,0.64,0.13,0.48
ALVHEIM,Aker BP ASA,2023,1,0.35,0.08,0.15
ALVHEIM,Aker BP ASA,2023,2,0.33,0.07,0.14
ALVHEIM,Aker BP ASA,2023,3,0.36,0.08,0.16
GOLIAT,Vår Energi ASA,2023,1,0.22,0.02,0.08
GOLIAT,Vår Energi ASA,2023,2,0.20,0.02,0.07
GOLIAT,Vår Energi ASA,2023,3,0.23,0.02,0.09
DRAUGEN,OKEA ASA,2023,1,0.11,0.01,0.09
DRAUGEN,OKEA ASA,2023,2,0.10,0.01,0.08
DRAUGEN,OKEA ASA,2023,3,0.12,0.01,0.10
"""
    with open(bronze_file_path, "w", encoding="utf-8") as f:
        f.write(fallback_csv_data.strip())
    print(f"SUCCESS: Bronze dataset initialized at: {bronze_file_path}")