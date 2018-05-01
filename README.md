# Final project for Applied Analytics: The Machine Learning Pipeline

Works on MacOS/Linux Python 3.6/R 

![](https://github.com/aredd-cmu/aa-project/blob/master/DataPipeline.png)

Steps to Run the Entire Pipeline:

1) Run [weather_scrape_daily.py](https://github.com/aredd-cmu/aa-project/blob/master/weather_scrape_daily.py) - The run will output a file entitled aggregated_data_daily.csv. This file needs to have a Daily directory in the home directory in order to work. 
2) Run [master.R](https://github.com/aredd-cmu/aa-project/blob/master/master.R) This will impute the missing values from the file and structure the weather data for merging with the financial data. It will output a file entitled final_weather_imputed.csv.
3) Run [futures_data.ipynb](https://github.com/aredd-cmu/aa-project/blob/master/futures_data.ipynb) This will pull, clean, and format the futures data for modeling. It will then perform a grid search over the data, run final models, and plot predictions against actual values for visual cross validation. 
