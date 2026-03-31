# scrape.py
# Cleaned version of your Colab notebook for automation

from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client
import os

# Supabase credentials (will read from environment variables)
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Scrape timestamp
scrape_time = datetime.now().isoformat()

# --- Scraper ---
url = "https://www.scrapethissite.com/pages/simple/"
response = requests.get(url, timeout=20)
soup = BeautifulSoup(response.text, "html.parser")

rows = []
for country in soup.select(".country"):
    name = country.select_one(".country-name").get_text(strip=True)
    capital = country.select_one(".country-capital").get_text(strip=True)
    population = country.select_one(".country-population").get_text(strip=True)
    area = country.select_one(".country-area").get_text(strip=True)

    rows.append({
        "name": name,
        "capital": capital,
        "population": pd.to_numeric(population, errors="coerce"),
        "area": pd.to_numeric(area, errors="coerce"),
        "scraped_at": scrape_time
    })

df = pd.DataFrame(rows)

# --- Enrichment ---
df["population_tier"] = df["population"].apply(
    lambda p: "Small" if p < 1_000_000 else ("Medium" if p < 50_000_000 else "Large")
)

df["area_tier"] = df["area"].apply(
    lambda a: "Small" if a < 10000 else ("Medium" if a < 1_000_000 else "Large")
)

# --- Insert into Supabase ---
rows_to_insert = []
for _, row in df.iterrows():
    rows_to_insert.append({
        "name": row["name"],
        "capital": row["capital"],
        "population": float(row["population"]),
        "area": float(row["area"]),
        "population_tier": row["population_tier"],
        "area_tier": row["area_tier"],
        "scraped_at": scrape_time
    })

supabase.table("countries").insert(rows_to_insert).execute()