import kagglehub
import pandas as pd
import os

path = kagglehub.dataset_download("anlgrbz/student-demographics-online-education-dataoulad")
print("Path to dataset files:", path)


for file in os.listdir(path):
    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join(path, file))
        print(file, df.shape)

from sqlalchemy import create_engine

user = 'root'
password = '7003890541'
port = 3306
host = 'localhost'
database = 'open_university'

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

for file in os.listdir(path):
    if file.endswith(".csv"):
        table = file.replace(".csv","")
        df = pd.read_csv(os.path.join(path, file))
        df.to_sql(table, engine, index=False, if_exists="replace")
