# Project Plan

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
This project investigates the correlation between weather conditions and traffic safety by analyzing how different weather conditions, including rain, snow, and extreme temperatures, impact accident rates. It analyzes accidents data from Berlin in the year 2020.

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
The project aims to determine the effect of weather conditions on traffic safety, which is essential for developing effective road safety policies and measures. By analyzing the correlation between weather conditions and accident rates, the project will provide insights into the extent to which weather conditions contribute to road accidents. Additionally, the report can raise awareness about the risks associated with driving in different weather conditions. This can help reduce the number of accidents and improve road safety for all road users.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Road traffic accidents by accident location in Berlin 2018
* Metadata URL: https://mobilithek.info/offers/-3519010126117898737
* Data URL: https://download.statistik-berlin-brandenburg.de/102d8fde949519f6/d99e618a1ec6/AfSBBB_BE_LOR_Strasse_Strassenverkehrsunfaelle_2018_Datensatz.csv
* Data Type: CSV

Road traffic accidents with personal injury by accident location with street name, GPS coordinates and LOR planning area in Berlin 2018; Accident month, weekday, hour, Accident type and category

### Datasource2: Open Meteo Weather API
* Metadata URL: https://open-meteo.com/en/docs/historical-weather-api
* Data URL: https://open-meteo.com/en/docs/historical-weather-api
* Data Type: JSON

OpenMeteo API is a free and open-source weather API that provides access to global and historical weather data. It offers a range of endpoints to retrieve weather forecasts, current conditions, and historical weather data. OpenMeteo API is easy to use and provides a variety of data formats, including csv & json making it a popular choice for developers who need to integrate weather data into their applications.



## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Collect and clean the dataset of traffic accidents and weather conditions. [https://github.com/MaeenBadea/2023-amse-template/issues/1]
2. Set up automated data pipelines for efficient data processing [https://github.com/MaeenBadea/2023-amse-template/issues/2]
3. Set up automated testing for the pipeline [https://github.com/MaeenBadea/2023-amse-template/issues/5]
4. Explore and analyze the data [https://github.com/MaeenBadea/2023-amse-template/issues/3]
5. utilize github actions for CI [https://github.com/MaeenBadea/2023-amse-template/issues/6]
6. Visualize the data, draw conclusions and create final report. [https://github.com/MaeenBadea/2023-amse-template/issues/4]
