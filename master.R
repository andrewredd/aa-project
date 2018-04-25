library(readr)
library(dplyr)
library(tidyr)
library(Amelia)

# set the seed but it doesn't change the amelia output
set.seed(1000)

# change this as needed
setwd("C:/Users/andre/OneDrive/School/CMU/Spring 2018/Applied Analytics/AA - Project/aa-project")

# load in the weather data. this is the output from the weather_agg jupyter notebook
data_unimput = read_csv("aggregated_data_daily.csv", col_types = list(X1 = col_integer(),
                                                                        station = col_character(),
                                                                        day = col_date(format = ""),
                                                                        max_temp_f = col_double(),
                                                                        min_temp_f = col_double(),
                                                                        max_dewpoint_f = col_double(),
                                                                        min_dewpoint_f = col_double(),
                                                                        precip_in = col_double(),
                                                                        avg_wind_speed_kts = col_double(),
                                                                        avg_wind_drct = col_double(),
                                                                        min_rh = col_double(),
                                                                        avg_rh = col_double(),
                                                                        max_rh = col_double(),
                                                                        climo_high_f = col_double(),
                                                                        climo_low_f = col_double(),
                                                                        climo_precip_in = col_double(),
                                                                        snow_in = col_double(),
                                                                        snowd_in = col_double()))



# impute values with amelia --- this has some issues. The imputation on some iterations is inserting 
# huge nonsensical numbers and other iterations appears to be the same
a.out = amelia(x = data_unimput %>% data.frame(), 
               parallel = 'multicore', 
               ncpus = 4, 
               idvars = c('X1'), 
               cs = 'station', 
               ts = 'day', 
               m=1)


# average the output from amelia by summing the datasets and then averaging them

rm(agg)
for (i in a.out$imputations){
  temp = i %>% as.data.frame()  
  if(!exists("agg")){
    agg = temp
  } else {
    agg = agg + temp
  }
}

agg[, c(-(1:3))] = agg[, c(-(1:3))] / length(a.out$imputations)

# spread out the data into station_variable form. each row is a day
final_weather_spread = agg %>% 
  gather(variable, value, -(station:day)) %>% 
  unite(temp, station, variable) %>% 
  spread(temp, value)

# function designed to normalize the data
normalize = function(vec){
  vec_sd = sd(vec)
  m = mean(vec)
  return((vec - m) / vec_sd)
}

# normalize the data
final_weather_spread[, -1] = lapply(final_weather_spread[, -1], normalize) %>% as.data.frame()

# write out a saved version of the weather data to avoid running the imputation every time you need this file
write_csv(final_weather_spread, "final_weather_spread.csv")

# read in the finance data
finance = read_csv("financialALL.csv")

lag_days = 30

target_finance = finance %>% 
  arrange(desc(Date)) %>% 
  select(Date, W1_Last, W2_Last, W3_Last, W4_Last, W5_Last) %>% 
  mutate(
    W1_Last_Lead = lag(W1_Last, lag_days),
    W2_Last_Lead = lag(W2_Last, lag_days),
    W3_Last_Lead = lag(W3_Last, lag_days),
    W4_Last_Lead = lag(W4_Last, lag_days),
    W5_Last_Lead = lag(W5_Last, lag_days)
  )

target_finance %>%  tail(100) %>%  View

# finance data doesn't have weekend data. we may want to add a step here that will either carry forward the 
# friday close or carry back the monday open

#removes WEAT and DBA as they have null values
final = final_weather_spread %>% inner_join(finance, by = c('day' = 'Date')) %>% select(-WEAT, -DBA)

# verify that there are not any missing values in the dataset
which((final %>% is.na() %>% colSums()) != 0)





