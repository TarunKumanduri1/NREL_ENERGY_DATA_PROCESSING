library(tidyverse)
library(here)
library(purrr)
process_cluster <- function(cluster_folder, cluster_number) {
  # Get all CSV files in the directory
  file_pattern <- file.path(here(), "data", "wind_long_clean_cluster", "Request_10", cluster_folder, "*.csv")
  all_files <- Sys.glob(file_pattern)
  if (length(all_files) == 0) {
    warning(paste("No CSV files found in cluster", cluster_number))
    return(NULL)
  }
  # Extract location information from file names
  locations <- data.frame(
    file_name = basename(all_files),
    stringsAsFactors = FALSE
  ) %>%
    separate(file_name, c("location_id", "lat", "lon", "year"), sep = "_", remove = FALSE) %>%
    mutate(year = sub(".csv", "", year)) %>%
    distinct(location_id, lat, lon)
  process_location_data <- function(location_id, lat, lon) {
    years <- c(2019, 2020)
    df_combined <- data.frame()
    for (year in years) {
      file_pattern <- paste0(location_id, "_", lat,"_", lon, "_", year, ".csv")
      file_path <- Sys.glob(file.path(here(), "data", "wind_long_clean_cluster", "Request_10", cluster_folder, file_pattern))
      if (length(file_path) == 0) {
        warning(paste("No file found for location", location_id, "in year", year))
        next
      }
      tryCatch({
        df <- read_csv(file_path[1], col_types = cols(.default = "c"))
        df_combined <- rbind(df_combined, df)
      }, error = function(e) {
        warning(paste("Error reading file for location", location_id, "in year", year, ":", e$message))
      })
    }
    if (nrow(df_combined) == 0) {
      warning(paste("No data found for location", location_id))
      return(c(lat, lon, NA))
    }
    siteid <- names(df_combined)[2]
    lat_col <- names(df_combined)[10]
    lon_col <- names(df_combined)[8]
    df_combined$SITEID <- siteid
    df_combined$Latitude <- ifelse(is.na(df_combined$Latitude), lat_col, df_combined$Latitude)
    df_combined$Longitude <- ifelse(is.na(df_combined$Longitude), lon_col, df_combined$Longitude)
    df_combined <- df_combined[, -c(8, 10)]  # Drop the 8th and 10th columns
    df_combined <- df_combined[-1, ]
    df_combined <- setNames(df_combined, c('Year', 'Month', 'Day', 'Hour', 'Minute', 'WindSpeed', 'Latitude', 'Longitude', 'SiteID'))
    df_combined$WindSpeed <- as.numeric(df_combined$WindSpeed)
    filtered_df <- df_combined %>% filter(WindSpeed < 4 | WindSpeed > 25)
    no_of_hours <- nrow(filtered_df)
    return(c(lat, lon, no_of_hours))
  }
  # Process all locations
  results <- locations %>%
    rowwise() %>%
    do(data.frame(t(process_location_data(.$location_id, .$lat, .$lon))))
  colnames(results) <- c("Latitude", "Longitude", "Hours")
  # Save results to CSV
  output_file <- file.path(here(), "data", "no_of_hours_clusters", "Request_10", paste0("processed_cluster_", cluster_number, ".csv"))
  write_csv(results, output_file)
  print(paste("Processing complete for cluster", cluster_number, ". Results saved to", output_file))
}
# Get all cluster folders
cluster_folders <- list.dirs(path = file.path(here(), "data", "wind_long_clean_cluster", "Request_10"), full.names = FALSE, recursive = FALSE)
# Use map to process each cluster folder
map2(cluster_folders, seq_along(cluster_folders), ~process_cluster(.x, .y))
print("All clusters processed and saved.")


#Matching clusters 
library(tidyverse)
library(here)

# Read plants_clusters.csv
df1 <- read_csv(here::here('data', 'clustered_files', 'plants_clusters.csv'))

# Function to find the closest point within a threshold
find_closest <- function(lat, lon, df, threshold) {
  df$diff <- abs(df$LAT - lat) + abs(df$LON - lon)
  closest <- df[which.min(df$diff), ]
  if (closest$diff <= threshold) {
    return(closest[, c("LAT", "LON", "Hours")])
  } else {
    return(data.frame(LAT = NA, LON = NA, Hours = NA))
  }
}

# Function to process a single CSV file with error handling
process_file <- function(file_path) {
  tryCatch({
    print(paste("Processing file:", file_path))
    
    # Read the CSV file
    df2 <- read_csv(file_path)
    
    # Rename columns in df2
    colnames(df2)[colnames(df2) == "Latitude"] <- "LAT"
    colnames(df2)[colnames(df2) == "Longitude"] <- "LON"
    
    # Perform initial join with small threshold
    small_threshold <- 0.1
    joined <- df1 %>%
      rowwise() %>%
      mutate(closest = list(find_closest(LAT, LON, df2, small_threshold))) %>%
      unnest(cols = c(closest), names_sep = "_df2_")
    
    # Rename columns for clarity
    joined <- joined %>%
      rename(
        LAT_point = LAT,
        LON_point = LON,
        LAT_NREL = closest_df2_LAT,
        LON_NREL = closest_df2_LON,
        Hours = closest_df2_Hours
      )
    
    # Determine which cluster has more non-NA values for both LAT_NREL and LON_NREL
    cluster_counts <- joined %>%
      group_by(cluster) %>%
      summarise(non_na_count = sum(!is.na(LAT_NREL) & !is.na(LON_NREL)))
    
    best_cluster <- cluster_counts %>%
      filter(non_na_count == max(non_na_count)) %>%
      pull(cluster)
    
    # Filter the joined dataframe for the best cluster
    filtered_joined <- joined %>%
      filter(cluster == best_cluster)
    
    # Create directory for output
    output_dir <- file.path(here(), "data", "Joined_data_plants_nrel")
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)
    }
    
    # Save the filtered result with appropriate filename
    output_filename <- paste0("cluster_", best_cluster, ".csv")
    write_csv(filtered_joined, file.path(output_dir, output_filename))
    
    # Print summary of join operation
    cat("Processing file:", basename(file_path), "\n")
    cat("Number of rows in df1:", nrow(df1), "\n")
    cat("Number of rows in df2:", nrow(df2), "\n")
    cat("Number of rows in joined dataframe:", nrow(joined), "\n")
    cat("Number of rows in filtered joined dataframe:", nrow(filtered_joined), "\n")
    cat("Best cluster:", best_cluster, "\n")
    cat("Number of non-NA matches (both LAT_NREL and LON_NREL) in best cluster:",
        cluster_counts$non_na_count[cluster_counts$cluster == best_cluster], "\n")
    cat("\n")
    
  }, error = function(e) {
    cat("Error processing file:", basename(file_path), "\n")
    cat("Error message:", conditionMessage(e), "\n\n")
  })
}

# Get all CSV files in the Request_8 directory
csv_files <- list.files(path = here("data", "no_of_hours_clusters", "Request_10"),
                        pattern = "\\.csv$",
                        full.names = TRUE)

# Use walk to process each CSV file
purrr::walk(csv_files, process_file)






df <- read_csv(here::here('data', 'clustered_files', 'plants_clusters.csv'))
cluster_counts_df <- df %>%
  group_by(cluster) %>%
  summarize(count = n())
write_csv(df, file.path(here(), "data", "clustered_files", "no_of_plants_clusters.csv"))



# #Other codes
# 
# # Single cluster Matching
# 
# Read CSV files
# df1 <- read_csv(here::here('data\\clustered_files', 'plants_clusters.csv'))
# df2 <- read_csv(here('data\\no_of_hours_clusters\\Individual requests\\cluster_98.csv'))
# # Rename columns in df2
# colnames(df2)[colnames(df2) == "Latitude"] <- "LAT"
# colnames(df2)[colnames(df2) == "Longitude"] <- "LON"
# # Function to find the closest point within a threshold
# find_closest <- function(lat, lon, df, threshold) {
#   df$diff <- abs(df$LAT - lat) + abs(df$LON - lon)
#   closest <- df[which.min(df$diff), ]
#   if (closest$diff <= threshold) {
#     return(closest[, c("LAT", "LON", "Hours")])
#   } else {
#     return(data.frame(LAT = NA, LON = NA))
#   }
# }
# # Perform initial join with small threshold
# small_threshold <- 0.1  # Adjust as needed
# joined <- df1 %>%
#   rowwise() %>%
#   mutate(closest = list(find_closest(LAT, LON, df2, small_threshold))) %>%
#   unnest(cols = c(closest), names_sep = "_df2_")
# # Rename columns for clarity
# joined <- joined %>%
#   rename(
#     LAT_point = LAT,
#     LON_point = LON,
#     LAT_NREL = closest_df2_LAT,
#     LON_NREL = closest_df2_LON,
#     Hours = closest_df2_Hours
#   )
# # Determine which cluster has more non-NA values for both LAT_NREL and LON_NREL
# cluster_counts <- joined %>%
#   group_by(cluster) %>%
#   summarise(non_na_count = sum(!is.na(LAT_NREL) & !is.na(LON_NREL)))
# best_cluster <- cluster_counts %>%
#   filter(non_na_count == max(non_na_count)) %>%
#   pull(cluster)
# # Filter the joined dataframe for the best cluster
# filtered_joined <- joined %>%
#   filter(cluster == best_cluster)
# # Create directory for output
# dir.create(file.path(here(), "data", "Joined_data_plants_nrel"), recursive = TRUE)
# # Save the filtered result with appropriate filename
# write_csv(filtered_joined, file.path(here(), "data", "Joined_data_plants_nrel", paste0("cluster_", best_cluster, ".csv")))
# # Print the first few rows of the result
# print(head(filtered_joined))
# # Print summary of join operation
# cat("Number of rows in df1:", nrow(df1), "\n")
# cat("Number of rows in df2:", nrow(df2), "\n")
# cat("Number of rows in joined dataframe:", nrow(joined), "\n")
# cat("Number of rows in filtered joined dataframe:", nrow(filtered_joined), "\n")
# cat("Best cluster:", best_cluster, "\n")
# cat("Number of non-NA matches (both LAT_NREL and LON_NREL) in best cluster:",
#     cluster_counts$non_na_count[cluster_counts$cluster == best_cluster], "\n")
# 
# 
# 




# Install and load required packages
#if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")
#library(dplyr)
# Read your dataframes
# df1 <- read_csv(here::here('data\\clustered_files', 'cluster_0.csv'))
# colnames(df1)[colnames(df1) == "LAT"] <- "Latitude" 
# colnames(df1)[colnames(df1) == "LON"] <- "Longitude" 
# df2 <- read_csv(here('data\\no_of_hours_clusters\\cluster_0\\wind_speed_summary_cluster_0.csv'))
# # Function to add rounded coordinates to a dataframe
# add_rounded_coords <- function(df, digits = 1) {
#   df$Latitude_rounded <- floor(df$Latitude, digits)
#   df$Longitude_rounded <- floor(df$Longitude, digits)
#   return(df)
# }
# # Add rounded coordinates to both dataframes
# df1 <- add_rounded_coords(df1)
# df2 <- add_rounded_coords(df2)
# # Join based on rounded coordinates
# #joined <- inner_join(df1, df2, by = c("Latitude_rounded", "Longitude_rounded"),
# #                     suffix = c("_df1", "_df2"))
# # If you want to keep all rows from df1, even unmatched ones, use left_join instead:
# joined <- left_join(df1, df2, by = c("Latitude_rounded", "Longitude_rounded"),
#                      suffix = c("_df1", "_df2"))
# # Remove temporary rounded columns
# # joined <- joined %>%
# #   select(-Latitude_rounded, -Longitude_rounded)
# # Rename columns to clarify which dataframe they came from
# joined <- joined %>%
#   rename(
#     Latitude_plant = Latitude,
#     Longitude_plant = Longitude,
#     Latitude_NREL = Latitude_df2,
#     Longitude_NREL = Longitude_df2
#   )
# # Save the result
# write.csv(joined, 'joined_output.csv', row.names = FALSE)
# # Print the first few rows of the result
# print(head(joined))
# # Print summary of join operation
# cat("Number of rows in df1:", nrow(df1), "\n")
# cat("Number of rows in df2:", nrow(df2), "\n")
# cat("Number of rows in joined dataframe:", nrow(joined), "\n")

#Sample file cleaning
# library(tidyverse)
# library(here)
# 
# folder_path <- file.path(here(), "data", "wind_long_clean_cluster", "cluster_0", "249536_37.75_-122.47_2019.csv")
# folder_path_2 <- file.path(here(), "data", "wind_long_clean_cluster", "cluster_0", "249536_37.75_-122.47_2020.csv")
# 
# df <- read_csv(folder_path)
# df_2 <- read_csv(folder_path_2)
# 
# df <- rbind(df, df_2)
# 
# siteid <- names(df)[2]
# lat_col <- names(df)[10]
# lon_col <- names(df)[8]
# 
# df$SITEID <- siteid
# # Replace NA values in Latitude and Longitude columns with the corresponding column name values
# df$Latitude <- ifelse(is.na(df$Latitude), lat_col, df$Latitude)
# df$Longitude <- ifelse(is.na(df$Longitude), lon_col, df$Longitude)
# df <- df[ , -c(8, 10)]  # Drop the 2nd and 4th columns
# df <- df[-1, ]
# df <- setNames(df, c('Year', 'Month', 'Day', 'Hour', 'Minute', 'WindSpeed', 'Latitude', 'Longitude', 'SiteID'))
# 
# df$WindSpeed <- as.numeric(df$WindSpeed)
# 
# no_of_hours_df <- df %>%
#   filter(WindSpeed < 4 | WindSpeed > 25)
# 
# no_of_hours <- nrow(no_of_hours_df)
# no_of_hours



# #  FOR 1 cluster
library(tidyverse)
library(here)
# Get all CSV files in the directory
file_pattern <- file.path(here(), "data", "wind_long_clean_cluster", "Individual requests", "cluster_162", "*.csv")
all_files <- Sys.glob(file_pattern)
# Extract location information from file names
locations <- data.frame(
  file_name = basename(all_files),
  stringsAsFactors = FALSE
) %>%
  separate(file_name, c("location_id", "lat", "lon", "year"), sep = "_", remove = FALSE) %>%
  mutate(year = sub(".csv", "", year)) %>%
  distinct(location_id, lat, lon)
process_location_data <- function(location_id, lat, lon) {
  years <- c(2019, 2020)
  df_combined <- data.frame()
  for (year in years) {
    file_name <- paste0(location_id, "_", lat, "_", lon, "_", year, ".csv")
    file_path <- file.path(here(), "data", "wind_long_clean_cluster", "Individual requests", "cluster_162", file_name)
    df <- read_csv(file_path)
    df_combined <- rbind(df_combined, df)
  }
  siteid <- names(df_combined)[2]
  lat_col <- names(df_combined)[10]
  lon_col <- names(df_combined)[8]
  df_combined$SITEID <- siteid
  df_combined$Latitude <- ifelse(is.na(df_combined$Latitude), lat_col, df_combined$Latitude)
  df_combined$Longitude <- ifelse(is.na(df_combined$Longitude), lon_col, df_combined$Longitude)
  df_combined <- df_combined[, -c(8, 10)]  # Drop the 8th and 10th columns
  df_combined <- df_combined[-1, ]
  df_combined <- setNames(df_combined, c('Year', 'Month', 'Day', 'Hour', 'Minute', 'WindSpeed', 'Latitude', 'Longitude', 'SiteID'))
  df_combined$WindSpeed <- as.numeric(df_combined$WindSpeed)
  filtered_df <- df_combined %>% filter(WindSpeed < 4 | WindSpeed > 25)
  no_of_hours <- nrow(filtered_df)
  return(c(lat, lon, no_of_hours))
}
# Process all locations
results <- locations %>%
  rowwise() %>%
  do(data.frame(t(process_location_data(.$location_id, .$lat, .$lon))))
colnames(results) <- c("Latitude", "Longitude", "Hours")
# Save results to CSV
write_csv(results, file.path(here(), "data", "no_of_hours_clusters", "Individual requests","cluster_162.csv"))
print("Processing complete.")




library(tidyverse)
library(here)

# Read the datasets
df1 <- read_csv(here::here('data', 'clustered_files', 'plants_clusters.csv'))
df2 <- read_csv(here('data', 'no_of_hours_clusters', 'Individual requests', 'cluster_162.csv'))

# Rename columns in df2
colnames(df2)[colnames(df2) == "Latitude"] <- "LAT"
colnames(df2)[colnames(df2) == "Longitude"] <- "LON"

# Function to find the closest point within a threshold
find_closest <- function(lat, lon, df, threshold) {
  df$diff <- abs(df$LAT - lat) + abs(df$LON - lon)
  closest <- df[which.min(df$diff), ]
  if (closest$diff <= threshold) {
    return(closest[, c("LAT", "LON", "Hours")])
  } else {
    return(data.frame(LAT = NA, LON = NA, Hours = NA))
  }
}

# Perform initial join with a small threshold
small_threshold <- 0.1  # Adjust as needed
joined <- df1 %>%
  rowwise() %>%
  mutate(closest = list(find_closest(LAT, LON, df2, small_threshold))) %>%
  unnest(cols = c(closest), names_sep = "_df2_")

# Rename columns for clarity
joined <- joined %>%
  rename(
    LAT_point = LAT,
    LON_point = LON,
    LAT_NREL = closest_df2_LAT,
    LON_NREL = closest_df2_LON,
    Hours = closest_df2_Hours
  )

# Filter the joined dataframe for cluster 115
filtered_joined <- joined %>%
  filter(cluster == 162)

# Create directory for output
output_dir <- file.path(here(), "data", "Joined_data_plants_nrel")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Save the filtered result with an appropriate filename
write_csv(filtered_joined, file.path(output_dir, "cluster_162.csv"))

# Print the first few rows of the result
print(head(filtered_joined))

# Print summary of join operation
cat("Number of rows in df1:", nrow(df1), "\n")
cat("Number of rows in df2:", nrow(df2), "\n")
cat("Number of rows in joined dataframe:", nrow(joined), "\n")
cat("Number of rows in filtered joined dataframe:", nrow(filtered_joined), "\n")
cat("Filtered for cluster:", 115, "\n")
cat("Number of non-NA matches (both LAT_NREL and LON_NREL) in cluster 115:",
    sum(!is.na(filtered_joined$LAT_NREL) & !is.na(filtered_joined$LON_NREL)), "\n")