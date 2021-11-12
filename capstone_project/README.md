# Data Engineering Capstone project
This project gathers together data on immigration into the US, 
as well as demographics of the destination cities and the source countries, 
including mean 21st century temperature.

# Data used
- **Immigration data** These data comes from the US National Tourism and Trade Office and details arriving immigrants to the US in the year 2016. These data are available [here](https://travel.trade.gov/research/reports/i94/historical/2016.html).
- **World Temperature Data** This dataset came from Kaggle. It includes average temperature recorded since 1750. For the purpose of this analysis, we only consider data from the 21st century.
- **U.S. City Demographic Data** This data comes from OpenSoft. It includes the population, median age, and population breakdown
- **Airport Code Table**: This data details airports

# Data created
The data are processed using PySpark and stored in Parquet format. 
The entire data pipeline can be run in `immigration_etl.ipynb`
A dictionary of the resulting data is available in `data_dictionary.md`.

# Intended use
The intended users of these data are immigrants who may be interested in deciding upon a destination in the US to move to. 
For example, a user may be interested in moving to a city where there is a large population of people from their own country already. Some example queries to this end can be found in `example_queries.ipynb`
