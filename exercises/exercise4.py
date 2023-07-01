import pandas as pd
import urllib.request as req
import zipfile
from sqlalchemy import create_engine, types

sql_types = {
    'Geraet': types.BIGINT,
    'Hersteller': types.TEXT,
    'Model': types.Text,
    'Monat': types.BIGINT,
    'Temperatur': types.Float,
    'Batterietemperatur': types.Float,
    'Batterietemperatur_1': types.Float,
    'Geraet aktiv': types.Text,
    'Geraet aktiv_1': types.Text


}

def celsius_to_fahrenheit(c):
    c = float(c)

    return (c * 9/5) + 32

def df_column_uniquify(df):
    df_columns = df.columns
    new_columns = []
    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = "{}_{}".format(item, counter)
        new_columns.append(newitem)
    df.columns = new_columns
    return df

class Pipeline:
    zip_path = "./mowesta_data.zip"
    def __init__(self):
    
        self.dataset_path = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
        self.df = None

    def execute(self):
        self.extract_data()
        self.transform_data()
        self.load_data()

    def extract_data(self):
        req.urlretrieve(self.dataset_path, Pipeline.zip_path)
        # zf = zipfile.ZipFile(Pipeline.zip_path) 
        # df = pd.read_csv(zf.open('data.csv'))
        # df.describe()
        if zipfile.is_zipfile(Pipeline.zip_path):
            try:
                zf = zipfile.ZipFile(Pipeline.zip_path) 
                self.df = pd.read_csv(zf.open('data.csv'), sep=';', names = range(500))
            except Exception as e:
                print('error reading csv')
        else:
            print("File is not a zip file")


    def transform_data(self):
        self.df = self.df.rename(columns=self.df.iloc[0]).drop(self.df.index[0])
        cols = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]
        self.df = self.df[cols]
        self.df = self.df.rename(columns={
            "Temperatur in °C (DWD)" : "Temperatur",
            "Batterietemperatur in °C" : "Batterietemperatur"

        })
        self.df = df_column_uniquify(self.df)
        self.df['Temperatur'] = self.df['Temperatur'].str.replace(',','.').map(celsius_to_fahrenheit)
        self.df['Batterietemperatur'] = self.df['Batterietemperatur'].str.replace(',', '.').map(celsius_to_fahrenheit)
        self.df['Batterietemperatur_1'] = self.df['Batterietemperatur_1'].str.replace(',', '.').map(celsius_to_fahrenheit)
        

    def load_data(self):
        engine = create_engine('sqlite:///temperatures.sqlite')
        self.df.to_sql('temperatures', engine, if_exists='replace',index= False, dtype= sql_types)
        

pipeline = Pipeline()
pipeline.execute()