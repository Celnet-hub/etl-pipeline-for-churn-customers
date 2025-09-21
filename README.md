# Customer Churn ETL Pipeline (with Logging)

This repository contains a simple ETL (Extract, Transform, Load) pipeline built with Python. It processes multiple CSV files containing customer churn data, standardizes their structure, logs the process, and loads the transformed data into both a CSV file and an SQLite database.

---

## 📂 Project Overview  

The pipeline performs four main tasks:  

1. **Extract**  
   - Reads all `.csv` files in the working directory (except the final output file).  
   - Combines them into a single pandas DataFrame.  

2. **Transform**  
   - Converts column names to lowercase.  
   - Sets the index name to `S/N` for clarity.  

3. **Load**  
   - Saves the transformed data to a CSV file.  
   - Loads the transformed data into an SQLite database table.  

4. **Logging**  
   - Logs each stage of the ETL process with timestamps in `log_file.txt`.  

---

## 🗂️ Files in the Repository  

| File | Description |
|-------|-------------|
| `etl.py` | The Python script containing the ETL pipeline. |
| `customers.db` | SQLite database created by the script (generated at runtime). |
| `transformed_data.csv` | Output CSV file containing the transformed data (generated at runtime). |
| `log_file.txt` | Log file recording ETL progress with timestamps (generated at runtime). |

---

## ⚙️ How It Works  

### Functions Overview

| Function                                   | Purpose                                                       |
| ------------------------------------------ | ------------------------------------------------------------- |
| `extract_from_csv(csv_file)`               | Reads a single CSV file into a pandas DataFrame.              |
| `extract()`                                | Reads all CSV files and concatenates them into one DataFrame. |
| `transform(data)`                          | Cleans and normalizes column names, sets index name.          |
| `load_data(destination, transformed_data)` | Saves data to CSV and loads into SQLite database.             |
| `log_progress(msg)`                        | Appends timestamped messages to `log_file.txt`.               |

---

## 🛠️ Requirements

* Python 3.13.0
* Libraries:

  * `pandas`
  * `sqlite3` (built-in)
  * `glob` (built-in)
  * `datetime` (built-in)

Install dependencies with:

```bash
pip install pandas
```

---

## 🚀 Running the Script

1. Place your input CSV files in the same directory as the script.
2. Run the script:

```bash
python etl.py
```

3. The outputs will be:

   * `transformed_data.csv`
   * SQLite database: `customers.db` (table: `churned_customers`)
   * `log_file.txt` containing ETL progress logs

---

## 📝 Notes

* The script automatically skips `transformed_data.csv` to avoid reprocessing its own output.
* Logging is handled by `log_progress()`, which appends timestamped messages to `log_file.txt`.
* You can customize:

  * `db_name` for the database filename.
  * `table_name` for the SQLite table name.
  * `final_result` for the output CSV filename.
  * `log_file` for the log filename.

---

## 📊 Data Columns

Expected columns in the source CSV files:

```
['State', 'Account length', 'Area code', 'International plan', 'Voice mail plan',
 'Number vmail messages', 'Total day minutes', 'Total day calls', 'Total day charge',
 'Total eve minutes', 'Total eve calls', 'Total eve charge', 'Total night minutes',
 'Total night calls', 'Total night charge', 'Total intl minutes', 'Total intl calls',
 'Total intl charge', 'Customer service calls', 'Churn']
```

---

## 🔧 Future Improvements

* Add data validation and error handling.
* Implement automated ETL scheduling.
* Expand logging to include error tracking.
* Create unit tests for ETL and logging functions.