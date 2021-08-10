# Consolidate data

# loading each dataset before combining them all in one
# combining the datasets in a single one, and saving to disk


library(tidyverse)
library(skimr)
library(janitor)


#
# Data cleaning
#

load_and_clean_2021_2020 <- function(path) {
  result <- readr::read_csv(
    path,
    col_types = list(
      ride_id = col_character(),
      rideable_type = col_character(),
      started_at = col_datetime(),
      ended_at = col_datetime(),
      start_station_name = col_character(),
      start_station_id = col_character(),
      end_station_name = col_character(),
      end_station_id = col_character(),
      start_lat = col_double(),
      start_lng = col_double(),
      end_lat = col_double(),
      end_lng = col_double(),
      member_casual = col_character()
    )
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  result$rideable_type = factor(result$rideable_type)
  result$member_casual = factor(result$member_casual)
  
  return(result)
}

load_and_clean_2019_2018 <- function(path) {
  result <- readr::read_csv(
    path,
    col_types = list(
      trip_id = col_character(),
      start_time = col_datetime(),
      end_time = col_datetime(),
      bikeid = col_number(),
      tripduration = col_number(),
      from_station_name = col_character(),
      from_station_id = col_character(),
      to_station_name = col_character(),
      to_station_id = col_character(),
      usertype = col_character(),
      gender = col_character(),
      birthyear = col_number()
    )
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  # change the column names to match the more recent ones
  result$ride_id = result$trip_id
  result$started_at = result$start_time
  result$ended_at = result$end_time
  result$start_station_name = result$from_station_name
  result$start_station_id = result$from_station_id
  result$end_station_name = result$to_station_name
  result$end_station_id = result$to_station_id
  
  result <- filter(result, usertype != "Dependent")
  
  result <- mutate(
    result, 
    member_casual = if_else(
      usertype == "Subscriber", 
      "member",
      if_else(
        usertype == "Customer",
        "casual",
        NULL
      )
    )
  )
  
  
  # Remove unused or redundant columns
  result <- select(
    result, 
    -trip_id, 
    -tripduration,
    -start_time,
    -end_time,
    -bikeid,
    -usertype,
    -gender,
    -birthyear,
    -from_station_name,
    -from_station_id,
    -to_station_name,
    -to_station_id
  )
  
  return(result)
}

# 2019 Q2 has different column names
load_and_clean_2019_q2 <- function(path) {
  result <- read_csv(
    path,
    skip = 1,
    col_names = c(
      "trip_id",
      "start_time",
      "end_time",
      "bikeid",
      "tripduration",
      "from_station_id",
      "from_station_name",
      "to_station_id",
      "to_station_name",
      "usertype",
      "gender",
      "birthyear"
    ),
    col_types = list(
      trip_id = col_character(),
      start_time = col_datetime(),
      end_time = col_datetime(),
      bikeid = col_number(),
      tripduration = col_number(),
      from_station_name = col_character(),
      from_station_id = col_character(),
      to_station_name = col_character(),
      to_station_id = col_character(),
      usertype = col_character(),
      gender = col_character(),
      birthyear = col_number()
    )
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  # change the column names to match the more recent ones
  result$ride_id = result$trip_id
  result$started_at = result$start_time
  result$ended_at = result$end_time
  result$start_station_name = result$from_station_name
  result$start_station_id = result$from_station_id
  result$end_station_name = result$to_station_name
  result$end_station_id = result$to_station_id
  
  result <- filter(result, usertype != "Dependent")
  
  result <- mutate(
    result, 
    member_casual = if_else(
      usertype == "Subscriber", 
      "member",
      if_else(
        usertype == "Customer",
        "casual",
        NULL
      )
    )
  )
  
  # Remove unused or redundant columns
  result <- select(
    result, 
    -trip_id, 
    -tripduration,
    -start_time,
    -end_time,
    -bikeid,
    -usertype,
    -gender,
    -birthyear,
    -from_station_name,
    -from_station_id,
    -to_station_name,
    -to_station_id
  )
  
  return(result)
}

load_and_clean_2017_q4 <- function(path) {
  result <- read.csv(
    path
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  # convert types
  result$trip_id <- as.character(result$trip_id)
  result$start_time <- strptime(result$start_time, "%m/%d/%Y %H:%M")
  result$end_time <- strptime(result$end_time, "%m/%d/%Y %H:%M")
  result$bikeid <- as.character(result$bikeid)
  result$from_station_id <- as.character(result$from_station_id)
  result$to_station_id <- as.character(result$to_station_id)

  # change the column names to match the more recent ones
  result$ride_id <- result$trip_id
  result$started_at <- result$start_time
  result$ended_at <- result$end_time
  result$start_station_name <- result$from_station_name
  result$start_station_id <- result$from_station_id
  result$end_station_name <- result$to_station_name
  result$end_station_id <- result$to_station_id
  
  result <- filter(result, usertype != "Dependent")
  
  result <- mutate(
    result, 
    member_casual = if_else(
      usertype == "Subscriber", 
      "member",
      if_else(
        usertype == "Customer",
        "casual",
        NULL
      )
    )
  )
  
  # Remove unused or redundant columns
  result <- select(
    result, 
    -trip_id, 
    -tripduration,
    -start_time,
    -end_time,
    -bikeid,
    -usertype,
    -gender,
    -birthyear,
    -from_station_name,
    -from_station_id,
    -to_station_name,
    -to_station_id
  )
  
  return(result)
}

load_and_clean_2017 <- function(path) {
  result <- read.csv(
    path
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  # convert types
  result$trip_id <- as.character(result$trip_id)
  result$start_time <- strptime(result$start_time, "%m/%d/%Y %H:%M:%S")
  result$end_time <- strptime(result$end_time, "%m/%d/%Y %H:%M:%S")
  result$bikeid <- as.character(result$bikeid)
  result$from_station_id <- as.character(result$from_station_id)
  result$to_station_id <- as.character(result$to_station_id)
  
  # change the column names to match the more recent ones
  result$ride_id <- result$trip_id
  result$started_at <- result$start_time
  result$ended_at <- result$end_time
  result$start_station_name <- result$from_station_name
  result$start_station_id <- result$from_station_id
  result$end_station_name <- result$to_station_name
  result$end_station_id <- result$to_station_id
  
  result <- filter(result, usertype != "Dependent")
  
  result <- mutate(
    result, 
    member_casual = if_else(
      usertype == "Subscriber", 
      "member",
      if_else(
        usertype == "Customer",
        "casual",
        NULL
      )
    )
  )
  
  # Remove unused or redundant columns
  result <- select(
    result, 
    -trip_id, 
    -tripduration,
    -start_time,
    -end_time,
    -bikeid,
    -usertype,
    -gender,
    -birthyear,
    -from_station_name,
    -from_station_id,
    -to_station_name,
    -to_station_id
  )
  
  return(result)
}

load_and_clean_2016 <- function(path) {
  result <- read.csv(
    path
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  # convert types
  result$trip_id <- as.character(result$trip_id)
  result$starttime <- strptime(result$starttime, "%m/%d/%Y %H:%M:%S")
  result$stoptime <- strptime(result$stoptime, "%m/%d/%Y %H:%M:%S")
  result$bikeid <- as.character(result$bikeid)
  result$from_station_id <- as.character(result$from_station_id)
  result$to_station_id <- as.character(result$to_station_id)
  
  # change the column names to match the more recent ones
  result$ride_id <- result$trip_id
  result$started_at <- result$starttime
  result$ended_at <- result$stoptime
  result$start_station_name <- result$from_station_name
  result$start_station_id <- result$from_station_id
  result$end_station_name <- result$to_station_name
  result$end_station_id <- result$to_station_id
  
  result <- filter(result, usertype != "Dependent")
  
  result <- mutate(
    result, 
    member_casual = if_else(
      usertype == "Subscriber", 
      "member",
      if_else(
        usertype == "Customer",
        "casual",
        NULL
      )
    )
  )
  
  # Remove unused or redundant columns
  result <- select(
    result, 
    -trip_id, 
    -tripduration,
    -starttime,
    -stoptime,
    -bikeid,
    -usertype,
    -gender,
    -birthyear,
    -from_station_name,
    -from_station_id,
    -to_station_name,
    -to_station_id
  )
  
  return(result)
}

load_and_clean_2016_06_01 <- function(path) {
  result <- read.csv(
    path
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  # convert types
  result$trip_id <- as.character(result$trip_id)
  result$starttime <- strptime(result$starttime, "%m/%d/%Y %H:%M")
  result$stoptime <- strptime(result$stoptime, "%m/%d/%Y %H:%M")
  result$bikeid <- as.character(result$bikeid)
  result$from_station_id <- as.character(result$from_station_id)
  result$to_station_id <- as.character(result$to_station_id)
  
  # change the column names to match the more recent ones
  result$ride_id <- result$trip_id
  result$started_at <- result$starttime
  result$ended_at <- result$stoptime
  result$start_station_name <- result$from_station_name
  result$start_station_id <- result$from_station_id
  result$end_station_name <- result$to_station_name
  result$end_station_id <- result$to_station_id
  
  result <- filter(result, usertype != "Dependent")
  
  result <- mutate(
    result, 
    member_casual = if_else(
      usertype == "Subscriber", 
      "member",
      if_else(
        usertype == "Customer",
        "casual",
        NULL
      )
    )
  )
  
  # Remove unused or redundant columns
  result <- select(
    result, 
    -trip_id, 
    -tripduration,
    -starttime,
    -stoptime,
    -bikeid,
    -usertype,
    -gender,
    -birthyear,
    -from_station_name,
    -from_station_id,
    -to_station_name,
    -to_station_id
  )
  
  return(result)
}




#
# Data loading
#

load_data_from_2021 <- function() {
  df_202106 <- load_and_clean_2021_2020("./raw_data/202106-divvy-tripdata/202106-divvy-tripdata.csv")
  final_df <- df_202106
  rm(df_202106)
  
  df_202105 <- load_and_clean_2021_2020("./raw_data/202105-divvy-tripdata/202105-divvy-tripdata.csv") 
  final_df <- bind_rows(final_df, df_202105)
  rm(df_202105)
  
  df_202104 <- load_and_clean_2021_2020("./raw_data/202104-divvy-tripdata/202104-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202104)
  rm(df_202104)
  
  df_202103 <- load_and_clean_2021_2020("./raw_data/202103-divvy-tripdata/202103-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202103)
  rm(df_202103)
  
  df_202102 <- load_and_clean_2021_2020("./raw_data/202102-divvy-tripdata/202102-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202102)
  rm(df_202102)
  
  df_202101 <- load_and_clean_2021_2020("./raw_data/202101-divvy-tripdata/202101-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202101)
  rm(df_202101)
  
  gc() # reclaiming some unused memory
  
  final_df <- distinct(final_df, ride_id, .keep_all=T)
  
  return(final_df)
}

load_data_from_2020 <- function() {
  df_202012 <- load_and_clean_2021_2020("./raw_data/202012-divvy-tripdata/202012-divvy-tripdata.csv")
  final_df <- df_202012
  rm(df_202012)
  
  df_202011 <- load_and_clean_2021_2020("./raw_data/202011-divvy-tripdata/202011-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202011)
  rm(df_202011)
  
  df_202010 <- load_and_clean_2021_2020("./raw_data/202010-divvy-tripdata/202010-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202010)
  rm(df_202010)
  
  df_202009 <- load_and_clean_2021_2020("./raw_data/202009-divvy-tripdata/202009-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202009)
  rm(df_202009)
  
  df_202008 <- load_and_clean_2021_2020("./raw_data/202008-divvy-tripdata/202008-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202008)
  rm(df_202008)
  
  df_202007 <- load_and_clean_2021_2020("./raw_data/202007-divvy-tripdata/202007-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202007)
  rm(df_202007)
  
  df_202006 <- load_and_clean_2021_2020("./raw_data/202006-divvy-tripdata/202006-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202006)
  rm(df_202006)
  
  df_202005 <- load_and_clean_2021_2020("./raw_data/202005-divvy-tripdata/202005-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202005)
  rm(df_202005)
  
  df_202004 <- load_and_clean_2021_2020("./raw_data/202004-divvy-tripdata/202004-divvy-tripdata.csv")
  final_df <- bind_rows(final_df, df_202004)
  rm(df_202004)
  
  df_2020Q1 <- load_and_clean_2021_2020("./raw_data/Divvy_Trips_2020_Q1/Divvy_Trips_2020_Q1.csv")
  final_df <- bind_rows(final_df, df_2020Q1)
  rm(df_2020Q1)
  
  gc() 
  
  return(final_df)
}

load_data_from_2019 <- function() {
  df_2019Q4 <- load_and_clean_2019_2018("./raw_data/Divvy_Trips_2019_Q4/Divvy_Trips_2019_Q4.csv")
  final_df <- df_2019Q4
  rm(df_2019Q4)
  
  df_2019Q3 <- load_and_clean_2019_2018("./raw_data/Divvy_Trips_2019_Q3/Divvy_Trips_2019_Q3.csv")
  final_df <- bind_rows(final_df, df_2019Q3)
  rm(df_2019Q3)
  
  df_2019Q2 <- load_and_clean_2019_q2("./raw_data/Divvy_Trips_2019_Q2/Divvy_Trips_2019_Q2.csv")
  final_df <- bind_rows(final_df, df_2019Q2)
  rm(df_2019Q2)
  
  df_2019Q1 <- load_and_clean_2019_2018("./raw_data/Divvy_Trips_2019_Q1/Divvy_Trips_2019_Q1.csv")
  final_df <- bind_rows(final_df, df_2019Q1)
  rm(df_2019Q1)
  gc()
  
  final_df <- distinct(final_df, ride_id, .keep_all=T)
  return(final_df)
}

load_data_from_2018 <- function() {
  df_2018Q4 <- load_and_clean_2019_2018("./raw_data/Divvy_Trips_2018_Q4/Divvy_Trips_2018_Q4.csv")
  final_df <- df_2018Q4
  rm(df_2018Q4)
  
  df_2018Q3 <- load_and_clean_2019_2018("./raw_data/Divvy_Trips_2018_Q3/Divvy_Trips_2018_Q3.csv")
  final_df <- bind_rows(final_df, df_2018Q3)
  rm(df_2018Q3)
  
  df_2018Q2 <- load_and_clean_2019_2018("./raw_data/Divvy_Trips_2018_Q2/Divvy_Trips_2018_Q2.csv")
  final_df <- bind_rows(final_df, df_2018Q2)
  rm(df_2018Q2)
  
  df_2018Q1 <- load_and_clean_2019_q2("./raw_data/Divvy_Trips_2018_Q1/Divvy_Trips_2018_Q1.csv")
  final_df <- bind_rows(final_df, df_2018Q1)
  rm(df_2018Q1)
  
  return(final_df)
}

load_data_from_2017 <- function() {
  
  df_2017Q4 <- load_and_clean_2017_q4("./raw_data/Divvy_Trips_2017_Q3Q4/Divvy_Trips_2017_Q4.csv")
  final_df <- df_2017Q4
  rm(df_2017Q4)
  
  df_2017Q3 <- load_and_clean_2017("./raw_data/Divvy_Trips_2017_Q3Q4/Divvy_Trips_2017_Q3.csv")
  final_df <- bind_rows(final_df, df_2017Q3)
  rm(df_2017Q3)
  
  df_2017Q2 <- load_and_clean_2017("./raw_data/Divvy_Trips_2017_Q1Q2/Divvy_Trips_2017_Q2.csv")
  final_df <- bind_rows(final_df, df_2017Q2)
  rm(df_2017Q2)
  
  df_2017Q1 <- load_and_clean_2017("./raw_data/Divvy_Trips_2017_Q1Q2/Divvy_Trips_2017_Q1.csv")
  final_df <- bind_rows(final_df, df_2017Q1)
  rm(df_2017Q1)
  
  return(final_df)
}

load_data_from_2016 <- function() {
  df_2016Q4 <- load_and_clean_2016("./raw_data/Divvy_Trips_2016_Q3Q4/Divvy_Trips_2016_Q4.csv")
  final_df <- df_2016Q4
  rm(df_2016Q4)
  
  df_2016Q3 <- load_and_clean_2016("./raw_data/Divvy_Trips_2016_Q3Q4/Divvy_Trips_2016_Q3.csv")
  final_df <- bind_rows(final_df, df_2016Q3)
  rm(df_2016Q3)
  
  df_201606 <- load_and_clean_2016_06_01("./raw_data/Divvy_Trips_2016_Q1Q2/Divvy_Trips_2016_06.csv")
  final_df <- bind_rows(final_df, df_201606)
  rm(df_201606)
  
  df_201605 <- load_and_clean_2016_06_01("./raw_data/Divvy_Trips_2016_Q1Q2/Divvy_Trips_2016_05.csv")
  final_df <- bind_rows(final_df, df_201605)
  rm(df_201605)
  
  df_201604 <- load_and_clean_2016_06_01("./raw_data/Divvy_Trips_2016_Q1Q2/Divvy_Trips_2016_04.csv")
  final_df <- bind_rows(final_df, df_201604)
  rm(df_201604)
  
  df_2016Q1 <- load_and_clean_2016_06_01("./raw_data/Divvy_Trips_2016_Q1Q2/Divvy_Trips_2016_Q1.csv")
  final_df <- bind_rows(final_df, df_2016Q1)
  rm(df_2016Q1)
  
  return(final_df)
}


#
# Main function
#
main <- function() {
  
  #data from 2021
  df_2021 <- load_data_from_2021()
  final_df <- df_2021
  rm(df_2021)
  gc()
  
  #data from 2020
  df_2020 <- load_data_from_2020()
  final_df <- bind_rows(final_df, df_2020)
  rm(df_2020)
  gc()
  
  #data from 2019
  df_2019 <- load_data_from_2019()
  final_df <- bind_rows(final_df, df_2019)
  rm(df_2019)
  gc()
  
  #data from 2018
  df_2018 <- load_data_from_2018()
  final_df <- bind_rows(final_df, df_2018)
  rm(df_2018)
  gc()
  
  #data from 2017
  df_2017 <- load_data_from_2017()
  final_df <- bind_rows(final_df, df_2017)
  rm(df_2017)
  gc()
  
  #data from 2016
  df_2016 <- load_data_from_2016()
  final_df <- bind_rows(final_df, df_2016)
  rm(df_2016)
  gc()
  
  final_df <- final_df %>% 
    distinct(ride_id, .keep_all=T) %>% 
    filter(started_at < ended_at)
  
  View(final_df)
  write_csv(final_df, "./cleaned_up_data/cyclist_data.csv")
}

main()



#
# Testing
#

test <- function() {
  # df_test <- load_data_from_2017()
  
  #df_test <- load_and_clean_2016_06("./raw_data/Divvy_Trips_2016_Q1Q2/Divvy_Trips_2016_06.csv")
  #df_test <- read.csv("./raw_data/Divvy_Trips_2016_Q1Q2/Divvy_Trips_2016_Q1.csv")
  
  #View(df_test)
  
  # glimpse(df_test)
  
  result <- read.csv(
    "./raw_data/Divvy_Trips_2017_Q3Q4/Divvy_Trips_2017_Q3.csv"
  ) %>% 
    janitor::clean_names() %>% 
    janitor::remove_empty()
  
  # convert types
  result$trip_id <- as.character(result$trip_id)
  result$start_time <- strptime(result$start_time, "%m/%d/%Y %H:%M:%S")
  result$end_time <- strptime(result$end_time, "%m/%d/%Y %H:%M:%S")
  result$bikeid <- as.character(result$bikeid)
  result$from_station_id <- as.character(result$from_station_id)
  result$to_station_id <- as.character(result$to_station_id)
  
  # change the column names to match the more recent ones
  result$ride_id <- result$trip_id
  result$started_at <- result$start_time
  result$ended_at <- result$end_time
  result$start_station_name <- result$from_station_name
  result$start_station_id <- result$from_station_id
  result$end_station_name <- result$to_station_name
  result$end_station_id <- result$to_station_id
  
  result <- mutate(
    result, 
    member_casual = if_else(
      usertype == "Subscriber", 
      "member",
      if_else(
        usertype == "Customer",
        "casual",
        NULL
      )
    )
  )
  
  # Remove unused or redundant columns
  result <- select(
    result, 
    -trip_id, 
    -tripduration,
    -start_time,
    -end_time,
    -bikeid,
    -usertype,
    -gender,
    -birthyear,
    -from_station_name,
    -from_station_id,
    -to_station_name,
    -to_station_id
  )
  
  
  
  View(result)
  rm(cyclist_data)
  gc()
}

