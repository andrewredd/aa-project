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


# setting thresholds on for variables based on records in the US with some buffer accommodate possiblity true extreme values
data_unimput$max_temp_f[data_unimput$max_temp_f >= 130] = NA
data_unimput$min_temp_f[data_unimput$min_temp_f <= -50] = NA
data_unimput$max_temp_f[data_unimput$max_temp_f <= min(data_unimput$min_temp_f)] = NA
data_unimput$min_temp_f[data_unimput$min_temp_f >= 130] = NA

data_unimput$max_dewpoint_f[data_unimput$max_dewpoint_f >= 90] = NA
data_unimput$min_dewpoint_f[data_unimput$min_dewpoint_f >= 90] = NA
data_unimput$max_dewpoint_f[data_unimput$max_dewpoint_f <= -20] = NA
data_unimput$min_dewpoint_f[data_unimput$min_dewpoint_f <= -20] = NA

data_unimput$precip_in[data_unimput$precip_in >= 70] = NA
data_unimput$precip_in[data_unimput$precip_in <= 0] = NA

data_unimput$snow_in[data_unimput$snow_in >= 75] = NA

summary(data_unimput)

# impute values with amelia --- this has some issues. The imputation on some iterations is inserting 
# huge nonsensical numbers and other iterations appears to be the same
a.out = amelia(x = data_unimput %>% data.frame(), 
               parallel = 'multicore', 
               ncpus = 4, 
               idvars = c('X1'), 
               cs = 'station', 
               ts = 'day', 
               emburn = c(5,100),
               m=5)


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

# write out a saved version of the weather data to avoid running the imputation every time you need this file
write_csv(final_weather_spread, "final_weather_imputed.csv")
