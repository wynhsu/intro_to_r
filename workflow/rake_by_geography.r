###############################################################################################################################
## Author: Amelia Bertozzi-Villa
## Description: For all-cause mortality, rake county-level estimates up to GBD' state-level estimates for any given year-sex;
##              OR rake tract-level estimates up to the county level for King county. 
##
## Inputs:
##        --lower_dir: location of the file at a lower geographic level (e.g. /ihme/us_counties/counties/mort/v2.2/mx_draws_1980_1_collapsed.rdata)
##        --upper_dir: location of the file at a higher geographic level (e.g "/ihme/us_counties/counties/mort/raking_inputs/prepped_gbd_draws.rdata")
##        --geo_agg_dir: file showing population weights of geographic aggregations
###################################################################################################################################

##---------------------------------------------
## Prep
##---------------------------------------------
rm(list=ls())
library(data.table)
library(reshape2)

main_dir <- commandArgs()[3]
year <- commandArgs()[4]
sex <- commandArgs()[5]

# #temp:
# main_dir <- "/home/j/Project/us_counties/mortality/all_cause/counties/bod2015/v2016_02_03/"
# year <- 1984
# sex <- 2

source("rake.r")
source("../settings.r")
root <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")

get_settings(main_dir)

##---------------------------------------------
## Load Data
##---------------------------------------------

print("reading in draws")
fname <- paste0("mx_draws_", area_var, "_", year, "_", sex, "_collapsed")
load(paste0(temp_dir, fname, ".rdata"))
orig_lower_draws <- copy(draws)
lower_draws <- copy(draws)
lower_draws <- draws[age %in% ages]
setnames(lower_draws, "area", area_var)

if (grepl(".rdata", raking_area_dir)) load(raking_area_dir) else draws <- fread(raking_area_dir)
upper_draws <- copy(draws); rm(draws)
setnames(upper_draws, c("area", "mx"), c(raking_area_var, paste0(raking_area_var, "_mx")))

#read in geo-agg data to map lower to upper geographical units
load(paste0(root, geoagg_files[raking_area_var]))
geo_agg <- copy(weights); rm(weights)
lower_draws <- merge(lower_draws, geo_agg, by=c(area_var, "year", "sex", "age"), all.x=T)

#merge
draws <- merge(lower_draws, upper_draws, by=c("year", "sex", "age", "sim", raking_area_var), all.x=T)

##---------------------------------------------
## Run raking
##---------------------------------------------

##rake
rake(draws, agg_var=raking_area_var, constant_vars <-c("year", "sex", "age", "sim"), replace_mx=T)

#reshape wide
setnames(draws, area_var, "area")

##---------------------------------------------
## Save
##---------------------------------------------

draws <- draws[, list(area,year,sex,age,sim,mx)]
save(draws, file=paste0(temp_dir, fname, "_raked.rdata"))
  

