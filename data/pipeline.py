import pandas as pd
from time import time 
import requests
from sqlalchemy import create_engine

class DataPipeline:
    def __init__(self):
        self.accidents_src = "https://download.statistik-berlin-brandenburg.de/102d8fde949519f6/d99e618a1ec6/AfSBBB_BE_LOR_Strasse_Strassenverkehrsunfaelle_2018_Datensatz.csv" # csv
        self.weather_src = "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2018-01-01&end_date=2018-12-31&daily=temperature_2m_mean,precipitation_sum,rain_sum,snowfall_sum,windspeed_10m_max&timezone=Europe%2FBerlin" # api
        self.dataset_df = None
        self.accidents_df = None
        self.weather_df = None

    def execute_pipeline(self):
        self.extract_data()
        self.transform_data()
        self.load_data()

    ############################################
    ############## ETL functions ###############
    ###########################################
    def extract_data(self):
        print("Start: Data extaction")
        t1 = time() 
        self.accidents_df = self.extract_csv(self.accidents_src, encoding="ISO-8859-1", sep = ";")
        self.weather_df = self.extract_api(self.weather_src)
        t2 = time()#
        print("Finish: Data extaction {} s ".format(t2-t1))

    
    def transform_data(self):
        print("Start: Data transform")
        t1 = time()

        self.accidents_df = self.massage_accidents(self.accidents_df)
        self.weather_df = self.massage_weather(self.weather_df)
        self.dataset_df  = pd.concat([self.accidents_df, self.weather_df], axis=1, join = "inner")

        t2 = time()
        print("Finish: Data transform {} s ".format(t2-t1))

    def load_data(self):
        print("Start: Data Loading")
        t1 = time() 

        engine = create_engine("sqlite:///berlin_dataset.sqlite")
        self.dataset_df.to_sql("berlin_dataset", engine, if_exists="replace")
        # self.dataset_df.to_csv("./data/berlin_dataset.csv")
        t2 = time()
        print("Finish: Data Loading {} s ".format(t2-t1))

    ############################################
    ############ helper functions ##############
    ############################################
    def extract_csv(self, csv_path, encoding = "utf-8", sep = ","):
        return pd.read_csv(csv_path, encoding=encoding, sep=sep)
    
    def extract_api(self, url):
        res = requests.get(url)
        return pd.DataFrame(res.json()['daily'])
    
    def massage_weather(self, weather_df):
        weather_df = weather_df.rename(columns={
            "temperature_2m_mean": 'temperature_mean',
            "windspeed_10m_max":'windspeed_max',
            })
        
        weather_df = weather_df.fillna(0)
        weather_df['date'] = pd.to_datetime(weather_df.time)
        weather_df['date'] = weather_df.date.dt.strftime('%Y/%m')
        weather_df = weather_df.groupby("date").agg({
                            'temperature_mean': 'mean',
                            'windspeed_max': 'max',
                            'precipitation_sum': 'sum',
                            'snowfall_sum': 'sum',
                            'rain_sum': 'sum'
                        })
        return weather_df
        
    def massage_accidents(self, accidents_df):
        #rename columns
        accidents_df = accidents_df.rename(columns={"STRASSE": "STREET", "UJAHR": "YEAR", "UMONAT":"MONTH",
                    "USTUNDE": "HOUR", "UWOCHENTAG":"WEEKDAY", "UKATEGORIE" : "category","IstFuss":"Pedestrains", "IstRad":"Bikes",
                    "IstPKW":"Cars", "ULICHTVERH": "Light Condition","IstKrad": "MotorBikes",
                    "IstGkfz": "Goods Trucks","IstSonstig":"Others"})
        #pad months with zero
        accidents_df['MONTH'] = accidents_df['MONTH'].apply(lambda x: str(x).rjust(2, '0'))
        #concat year + month
        accidents_df['date'] = accidents_df['YEAR'].astype(str) + "/" + accidents_df['MONTH'].astype(str)
        #drop unwanted columns
        accidents_df = accidents_df.drop(columns=['OBJECTID', 'LAND',"LOR", "STREET","LOR_ab_2021","YEAR","MONTH","HOUR","WEEKDAY","STRZUSTAND", "LINREFX",
                  	"LINREFY",	"XGCSWGS84",	"YGCSWGS84","UART","UTYP1","BEZ","Light Condition"])
        
        categories  = accidents_df.groupby(["date", "category"]).size().unstack(fill_value=0).rename(columns = {1:"Fatal",2: "serious_injury",3: "minor_injury"})            
        # aggregate data monthly
        grouped = accidents_df.groupby("date").agg({"Bikes":"sum",	"Cars":"sum",
                         	"Pedestrains":"sum","MotorBikes":"sum",
                         	"Goods Trucks":"sum","date": "count",
                            "Others":"sum"
                                	})
        grouped = grouped.rename(columns={"date":"total"})
        df = pd.concat([grouped, categories], axis=1).reindex(grouped.index)
        
        new_cols_order = df.columns.tolist()
        new_cols_order.remove('total')
        new_cols_order = ['total'] + new_cols_order
        return df[new_cols_order]

pipeline = DataPipeline()
pipeline.execute_pipeline()