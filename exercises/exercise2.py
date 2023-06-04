import pandas as pd
from sqlalchemy import create_engine, types
								
sql_types = {
    'index': types.BIGINT,
    'EVA_NR': types.BIGINT,
    'DS100': types.TEXT,
    'IFOPT': types.TEXT,
    'NAME': types.TEXT,
    'Verkehr': types.TEXT,
    'Laenge': types.FLOAT,
    'Breite': types.FLOAT,
    'Betreiber_Name': types.TEXT,
    'Betreiber_Nr': types.BIGINT,

}
class DataPipeline:
    def __init__(self):
        self.dataset_df = None
        self.dataset_src = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"

    def execute_pipeline(self):
        self.extract_data(self.dataset_src)
        self.transform_data()
        self.load_data()

    def extract_data(self, src):
        self.dataset_df= pd.read_csv(src, sep=";")
    
    def transform_data(self):
        self.dataset_df = self.dataset_df.drop(columns=['Status'])
        self.dataset_df['Laenge'] = self.dataset_df['Laenge'].str.replace(',','.').astype(float)
        self.dataset_df['Breite'] = self.dataset_df['Breite'].str.replace(',','.').astype(float)
        
        valid_rows = (self.dataset_df['Verkehr'].isin(['RV','FV','nur DPN']) & 
                    self.dataset_df['Laenge'].astype(float).between(-90,90)
                    & self.dataset_df['Breite'].astype(float).between(-90,90) 
                    & self.dataset_df['IFOPT'].str.match("\w{2}\d*:\d*(:\d*)*"))
        
        self.dataset_df = self.dataset_df[valid_rows]
        self.dataset_df = self.dataset_df.dropna()


    def load_data(self):
        if self.dataset_df is not None:
            engine = create_engine("sqlite:///trainstops.sqlite")
            self.dataset_df.to_sql('trainstops',engine ,  if_exists='replace', dtype= sql_types)
        else:
            raise ValueError('DF is empty, Check data extraction')


dp = DataPipeline()
dp.execute_pipeline()