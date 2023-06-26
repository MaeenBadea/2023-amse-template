import pytest
from pipeline import DataPipeline
import os


@pytest.fixture
def etl_pipeline():
    # Create an instance of the ETLPipeline for testing
    return DataPipeline()

@pytest.mark.read
def test_csv_reading(etl_pipeline):
    etl_pipeline.extract_data()
    assert not etl_pipeline.accidents_df.empty # 
    
@pytest.mark.read
def test_api_reading(etl_pipeline):
    etl_pipeline.extract_data()
    assert not etl_pipeline.weather_df.empty # 


# check data has been transformed
# columns from both datasets exist & checks translation of columns from german -> english
def test_data_transform(etl_pipeline):
    # Test data transformations
    etl_pipeline.extract_data()
    etl_pipeline.transform_data()
    assert (not etl_pipeline.dataset_df.empty) and ({'snowfall_sum','Cars'} <= set(etl_pipeline.dataset_df.keys()))
    
# system test
# test the whole pipeline execution
def test_sqlite_database_creation(etl_pipeline):
    # Test SQLite database creation
    etl_pipeline.execute_pipeline()
    assert os.path.exists("./berlin_dataset.sqlite")

