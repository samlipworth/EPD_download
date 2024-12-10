#!/usr/bin/env Rscript


library(jsonlite)
library(dplyr)
library(optparse)
library(readr)

option_list <- list(
  make_option("--bnf_drug", type = "character", help = "BNF chemical substance code (e.g., '0501013K0')"),
  make_option("--outfile", type = "character", help = "Output file path (e.g., 'output.tsv')")
)

opt <- parse_args(OptionParser(option_list = option_list))

if (is.null(opt$bnf_drug) || is.null(opt$outfile)) {
  stop("Both --bnf_drug and --outfile options are required.")
}

bnf_chemical_substance <- opt$bnf_drug
outfile <- opt$outfile
df <- data.frame()

base_endpoint <- "https://opendata.nhsbsa.net/api/3/action/"
package_show_method <- "package_show?id="
action_method <- "datastore_search_sql?"
dataset_id <- "english-prescribing-data-epd"


metadata_response <- jsonlite::fromJSON(paste0(
  base_endpoint,
  package_show_method,
  dataset_id
))

resources_table <- metadata_response$result$resources


for (year in 2014:2023) {
  # Filter by year to make the API task manageable
  resource_name_list <- resources_table$name[grepl(as.character(year), resources_table$name)]
  
  if (length(resource_name_list) == 0) {
    message(paste("No datasets found for the year", year))
    next
  }
  
  
  for (month in resource_name_list) {
    print(paste("Processing month:", month))
    
    # then by regional office equally otherwise the API fails
    regional_query <- paste0(
      "SELECT DISTINCT REGIONAL_OFFICE_NAME FROM `", month, "`"
    )
    regional_query_url <- paste0(base_endpoint, action_method, "resource_id=", month, "&sql=", URLencode(regional_query))
    
    # get all unique regions
    regions <- tryCatch({
      response <- fromJSON(regional_query_url)
      response$result$result$records %>% pull(REGIONAL_OFFICE_NAME)
    }, error = function(e) {
      message("Error fetching regional offices: ", e)
      NULL
    })
    
    if (is.null(regions) || length(regions) == 0) {
      message("No regional office data found for month: ", month)
      next
    }
    
    
    for (region in regions) {
      print(paste("Processing region:", region, "for month:", month))
      
      limit <- 5000  # Number of rows per page
      offset <- 0    # Starting row index
      has_more_data <- TRUE
      
      while (has_more_data) {
        # Build query for the current region and offset
        paginated_query <- paste0(
          "SELECT * FROM `", month, "` ",
          "WHERE bnf_chemical_substance = '", bnf_chemical_substance, "' ",
          "AND REGIONAL_OFFICE_NAME = '", region, "' ",
          "LIMIT ", limit, " OFFSET ", offset
        )
        
        # Construct API call URL
        paginated_api_call <- paste0(
          base_endpoint,
          action_method,
          "resource_id=", month, "&sql=",
          URLencode(paginated_query)
        )
        
        # Fetch data
        response <- tryCatch({
          fromJSON(paginated_api_call)
        }, error = function(e) {
          message("Error fetching data for region ", region, ": ", e)
          NULL
        })
        
       
        if (is.null(response)) {
          message("Skipping offset ", offset, " for region ", region, " in month ", month)
          has_more_data <- FALSE
          next
        }
        
        
        tmp_df <- response$result$result$records
        
        
        if (!is.null(nrow(tmp_df)) && nrow(tmp_df) > 0) {
          df <- bind_rows(df, tmp_df)
        } else {
          message("No data found for offset ", offset, " in region ", region, " for month ", month)
        }
        
        
        has_more_data <- !is.null(nrow(tmp_df)) && nrow(tmp_df) == limit
        offset <- offset + limit  # Move to the next chunk
        
        print(paste("Processed offset:", offset, "for region:", region, "in month:", month))
      }
    }
  }
}

# Save output
if (nrow(df) > 0) {
  write_tsv(df, outfile)
  message("Data successfully saved to ", outfile)
} else {
  message("No data found for the specified BNF drug.")
}
