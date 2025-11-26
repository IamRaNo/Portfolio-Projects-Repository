import gdown
import os
import pandas as pd
from sqlalchemy import create_engine
import pymysql

folder_url = "https://drive.google.com/drive/folders/1UHH9wmZ137_IK8zTSQbWcQd7dbXYEC1c?usp=drive_link"
download_path = "synthea_data"

# download entire folder
gdown.download_folder(folder_url, output=download_path, quiet=False)

# create database synthea if not exists
conn = pymysql.connect(host='localhost', user='root', password='7003890541')
cur = conn.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS synthea")
conn.close()

engine = create_engine("mysql+pymysql://root:7003890541@localhost:3306/synthea")

# loop through all files and load into SQL
for file in os.listdir(download_path):
    if file.endswith(".csv"):
        filepath = os.path.join(download_path, file)
        df = pd.read_csv(filepath)
        table = file.replace(".csv","")
        print("Loading:", table, df.shape)

        df.to_sql(table,
                  engine,
                  index=False,
                  if_exists="replace",
                  chunksize=10000)

