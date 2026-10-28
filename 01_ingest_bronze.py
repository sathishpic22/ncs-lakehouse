import datetime
import os
import requests

print("=== Step 1: Ingesting Raw Production Data (Bronze Layer) ===")

# Safe directory resolution for both local and Databricks
base_dir = (
    os.path.dirname(os.path.abspath(__file__))
    if "__file__" in globals()
    else os.getcwd()
)

# Open data endpoint from the Norwegian Offshore Directorate (Sodir)
DATA_URL = "https://factpages.sodir.no/public?/FactPages/TableView/field_production_monthly&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&rs:Format=CSV&Top100=false&IpAddress=NotApplicable&CultureCode=en"

# Bronze partition path: data/bronze/ingest_date=YYYY-MM-DD
today_str = datetime.date.today().isoformat()
bronze_folder = os.path.join(base_dir, f"data/bronze/ingest_date={today_str}")
os.makedirs(bronze_folder, exist_ok=True)

bronze_file_path = os.path.join(
    bronze_folder, "ncs_monthly_field_production.csv"
)

print(f"Downloading monthly offshore production data from Sodir...")
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
}
response = requests.get(DATA_URL, headers=headers, timeout=60)

if response.status_code == 200:
    with open(bronze_file_path, "wb") as f:
        f.write(response.content)
    print(f"SUCCESS: Raw Bronze CSV saved to:\n  -> {bronze_file_path}")
else:
    raise RuntimeError(
        f"Failed to fetch data from Sodir. Status code: {response.status_code}"
    )