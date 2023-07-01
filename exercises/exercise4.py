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
    'Geraet aktiv': types.Text,
}

def celsius_to_fahrenheit(c):
    c = c.replace(',','.')
    c = float(c)
    return (c * 9/5) + 32


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

        try:
            zf = zipfile.ZipFile(Pipeline.zip_path) 
            self.df = pd.read_csv(zf.open('data.csv'), sep=';', names = range(500))
        except Exception as e:
            print('error reading csv', e)



    def transform_data(self):
        self.df = self.df.rename(columns=self.df.iloc[0]).drop(self.df.index[0])
        cols = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]
        self.df = self.df[cols]
        self.df = self.df.rename(columns={
            "Temperatur in °C (DWD)" : "Temperatur",
            "Batterietemperatur in °C" : "Batterietemperatur"

        })
        self.df = self.df.iloc[:, ~self.df.columns.duplicated()]
        self.df['Temperatur'] = self.df['Temperatur'].apply(celsius_to_fahrenheit)
        self.df['Batterietemperatur'] = self.df['Batterietemperatur'].apply(celsius_to_fahrenheit)
        

    def load_data(self):
        engine = create_engine('sqlite:///temperatures.sqlite')
        self.df.to_sql('temperatures', engine, if_exists='replace',index= False, dtype= sql_types)
        

pipeline = Pipeline()
pipeline.execute()