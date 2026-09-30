# 📂 Dataset — NYC Yellow Taxi Trip Records

## Dataset Name
**NYC TLC Yellow Taxi Trip Record Data**

## Official Source
- **Publisher:** New York City Taxi & Limousine Commission (TLC)
- **URL:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Direct Parquet Links:** https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet

## Download Instructions

### Option 1 — Automatic Sample Download (Recommended for Local Testing)
Run the ingestion script to download a single month's data (~40 MB Parquet):
```bash
python src/ingestion.py
```
This downloads **January 2024** by default. Configure the month/year in the script.

### Option 2 — Manual Download
1. Visit the [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
2. Under **Yellow Taxi Trip Records**, select the desired year and month.
3. Download the `.parquet` file.
4. Place it in this `data/` directory.

### Option 3 — Full Dataset on Databricks
Mount the full dataset directly from cloud storage or upload multiple months:
```python
# Databricks example
df = spark.read.parquet("dbfs:/FileStore/tables/yellow_tripdata_*.parquet")
```

## Expected File Format
- **Format:** Apache Parquet (columnar)
- **Compression:** Snappy (default from TLC)
- **Naming:** `yellow_tripdata_YYYY-MM.parquet`

## Required Columns

| Column                 | Type      | Description                          |
|------------------------|-----------|--------------------------------------|
| VendorID               | int       | TPEP provider (1=CMT, 2=VeriFone)   |
| tpep_pickup_datetime   | timestamp | Pickup date and time                 |
| tpep_dropoff_datetime  | timestamp | Drop-off date and time               |
| passenger_count        | double    | Number of passengers                 |
| trip_distance          | double    | Trip distance in miles               |
| RatecodeID             | double    | Rate code (1=Standard, etc.)         |
| store_and_fwd_flag     | string    | Store-and-forward flag               |
| PULocationID           | int       | Pickup TLC Taxi Zone ID              |
| DOLocationID           | int       | Drop-off TLC Taxi Zone ID            |
| payment_type           | int       | Payment method (1=Credit, 2=Cash)    |
| fare_amount            | double    | Meter fare in USD                    |
| extra                  | double    | Extras and surcharges                |
| mta_tax                | double    | MTA tax                              |
| tip_amount             | double    | Tip amount (credit card only)        |
| tolls_amount           | double    | Tolls                                |
| improvement_surcharge  | double    | Improvement surcharge                |
| total_amount           | double    | Total charged to passenger           |
| congestion_surcharge   | double    | Congestion surcharge                 |
| Airport_fee            | double    | Airport fee                          |

## Taxi Zone Lookup
Download the **Taxi Zone Lookup Table** (CSV) from the same TLC page for zone name mapping:
- File: `taxi_zone_lookup.csv`
- Columns: `LocationID`, `Borough`, `Zone`, `service_zone`

## Approximate Dataset Size
| Scope            | Records       | File Size       |
|------------------|---------------|-----------------|
| 1 month          | ~3 million    | ~40-50 MB       |
| 1 year           | ~35 million   | ~500 MB         |
| Full (2009–2024) | ~1.5 billion  | ~30 GB          |

> ⚠️ **Do NOT commit Parquet/CSV data files to Git.** They are excluded via `.gitignore`.
