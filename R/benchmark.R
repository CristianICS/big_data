library(sf)
library(dplyr)
library(bench)
library(parallel)
library(mapSpain)

# Note: This is not an R project so we need to configure the root path
root <- "C:/copia_seguridad/cursos/CDG/practicas/04_github/big_data_training/"
data_folder <- file.path(root, "raw_data/zgz_daily")
# Get file paths
zgz_daily_files <- list.files(data_folder, full.names = TRUE)

# Obtain the region of interest
zgz <- esp_get_prov("Zaragoza")
# Convert the CRS to the daily gpkg crs
zgz_25830 <- st_transform(zgz, 25830)

# Create the function to do the averaging by file in parallel
compute_avg <- function(file_path) {
  
  # Get file name (the layer of the GPKG file)
  lname <- st_layers(file_path)[1, 1]
  
  # Construct query
  cols <- paste(c("MeanTemperature", "Precipitation", "MeanRelativeHumidity",
                  "day", "geom"), collapse = ",")
  sql_q <- paste0(c('SELECT ', cols, ' FROM \"', lname, '\"'), collapse="")
  
  # Load only Zaragoza points
  i_df <- st_read(file_path, quiet = TRUE,
                  query = sql_q,
                  wkt_filter = st_as_text(zgz_25830$geometry)
  )
  # Compute the average
  i_avg <- i_df |>
    st_drop_geometry() |>
    group_by(day) |>
    summarise(across(where(is.numeric), mean, na.rm = TRUE))
  
  # Compute and return averages
  return(i_avg)
}

# Benchmark how many cores are optimal
averaging <- bench::mark(
  "ncores_1" = mclapply(zgz_daily_files, compute_avg, mc.cores = 1),
  "ncores_2" = mclapply(zgz_daily_files, compute_avg, mc.cores = 2),
  "ncores_3" = mclapply(zgz_daily_files, compute_avg, mc.cores = 3)
)