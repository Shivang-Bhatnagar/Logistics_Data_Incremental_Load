# 🚚 Incremental Logistics Data Pipeline Using Databricks & Delta Lake
This project automates an **incremental data load pipeline** for logistics tracking data using **Apache Spark**, **Delta Lake**, and **Google Cloud Platform (GCP)** with **Databricks Workflows**.

> 🎯 Real-time file processing, Delta merge logic, and data archival – all orchestrated seamlessly.

---

## 🔧 Tech Stack

- **Apache Spark** on **Databricks**
- **Delta Lake** for ACID-compliant data
- **Google Cloud Storage** (GCS)
- **DBFS** (Databricks File System)
- **Python 3**

---

## 📈 Pipeline Architecture

1. File is uploaded to `staging_zn` directory in DBFS (mounted from GCS).
2. A Databricks job is triggered.
3. CSV file is read and written to a Delta staging table (`stage_zn`).
4. Files are then archived to `archive/`.
5. A Delta merge updates the `target_zn` table using the staging data.
6. Old records are deleted and new records inserted via `MERGE`.

---

## 🗂️ Folder Contents

| File / Folder        | Description                                  |
|----------------------|----------------------------------------------|
| `stage_data_load.ipynb`     | Reads CSV from DBFS and loads it to staging Delta table |
| `target_data_load.ipynb`   | Merges stage data to final target Delta table          |
| `README.md`          | Project overview and instructions            |
| `requirements.txt`   | Optional dependencies list                   |

---

## 🚀 How to Run

Upload `.csv` files to:

dbfs:/FileStore/staging_zn/

Then execute the scripts in Databricks or as a job sequence.

---

## 🔄 Merge Logic

```python
target_table.alias("target") \
    .merge(stage_df.alias("stage"), "stage.tracking_num = target.tracking_num") \
    .whenMatchedDelete() \
    .execute()

stage_df.write.format("delta").mode("append").saveAsTable(target_table_name)
```
## 📦 requirements.txt (optional)
pyspark>=3.3.0

delta-spark>=2.4.0

## 💡 Feel free to fork, contribute, or ⭐ this project if you find it helpful — Happy Data Engineering! 🚀

