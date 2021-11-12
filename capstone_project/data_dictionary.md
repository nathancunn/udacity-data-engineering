# Table schema - snowflake
## Fact table - immigration
This table outlines all imigrations by air into the US in April of 2016.
  - immigrant_id [int]
  - arrival_city_code [int] A code outlining the city through which the immigrant entered the US
  - airport_code [int] A code outlining the airport in whicht he immigrant arrived
  - arrival_date [timestamp] A date object indicating the date of arrival
  - departure_date [timestamp] A date object indicating when the immigrant departed the country, if applicable
  - source_country [int] A code indicating the country from which the immigrant came
  - age [int] The age of the immigrant upon arrival in year
  - gender [string] The declared gender of the immigrant
  
## Dimension table - city
This table provides details on American cities
  - city_code [int] A code identifying cities, corresponding to arrival_city_code in immigration
  - city_name [string] The city name
  - population [int] The population of the city
  - male_population [int] The male population of the city
  - female_population [int] The female population of the city
  - mean_temperature [float] The mean recorded temperature in this cty throughout the 21st century
  - american_indian_alaska [int] The population of the city of American Indian, or Alaska Native descent
  - asian [int] The Asian population of the city
  - black_african_american [int] The Black/African American population of the city
  - hispanic_latino [int] The Hispanic/Latino populaton of the city
  - white [int] The white population of the city
  
## Dimension table - airport
This table provides details on American airports
  - airport_code [int] A code identifying the airport
  - airport_name [string] The airport name
  - city_code [int] The 
  - airport_type [string]
  
## Dimension table - country
This table provides details on non-US countries
  - country_code [int]
  - country_name [string]
  - mean_temperature [float]