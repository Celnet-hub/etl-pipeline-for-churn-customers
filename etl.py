import glob
import pandas as pd
from datetime import datetime
import sqlite3

log_file = "log_file.txt"
final_result = "transformed_data.csv"
db_name = "customers.db"
table_name = "churned_customers"

# Get columns
# columns = pd.read_csv('churn-bigml-20.csv')
# print(columns.columns)

###### Extraction ########


def extract_from_csv(csv_file):
    dataframe = pd.read_csv(csv_file)
    return dataframe


def extract():
    # create an empty dataframe to hold extracted data
    extracted_data = pd.DataFrame(columns=['State', 'Account length', 'Area code', 'International plan',
                                           'Voice mail plan', 'Number vmail messages', 'Total day minutes',
                                           'Total day calls', 'Total day charge', 'Total eve minutes',
                                           'Total eve calls', 'Total eve charge', 'Total night minutes',
                                           'Total night calls', 'Total night charge', 'Total intl minutes',
                                           'Total intl calls', 'Total intl charge', 'Customer service calls',
                                           'Churn'])

    # begin processing files
    for csvfile in glob.glob("*.csv"):
        if csvfile != final_result:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(
                extract_from_csv(csvfile))], ignore_index=True)

    return extracted_data


############### Transformation ######################

def transform(data):
    data.columns = data.columns.str.lower()
    data.index.name = "S/N"
    return data


############### Load to CSV and to SQL #################

def load_data(destination, transformed_data):
    transformed_data.to_csv(destination)

    #Load to SQL
    conn = sqlite3.connect(db_name)
    transformed_data.to_sql(table_name, conn, if_exists='replace', index=False) #index= Fasle prevents pandas from writing the dataframe index as column in the database's table
    conn.close()
    print("Loaded to sqlite table")


def log_progress(msg):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as logs:
        logs.write(timestamp + "," + msg + "\n")




######## Begin ETL ###########
log_progress("Beginning ETL process")
extracted_data = extract()

log_progress("Extract completed \n")
log_progress("Beginning transformation \n")

transformed_data = transform(extracted_data)


log_progress("Transformation completed \n")
log_progress("Beginning Load \n")

load_data(final_result, transformed_data)

log_progress("Data loaded to csv file")
