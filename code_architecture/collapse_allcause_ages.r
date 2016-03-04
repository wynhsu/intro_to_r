#############################################################################################
## Author: Amelia Bertozzi-Villa
## Description: Until the COD team finds time to make this change, all the cause-specific
## files we get have 80 as the terminal age group, whereas all-cause results have 85 as the
## terminal age group. Thus, we need to collapse the all-cause data down to have 80 as the
## terminal age group. 
##
## Inputs:
##        -- year: year for which we want to run the data
##        --sex: 1=male, 2=female
##        -- allcause_temp_dir : clustertmp file where population draws live
##        --allcause_pop_dir : single population file (with 85+ ages) on J
#############################################################################################


##---------------------------------------------
## Prep
##---------------------------------------------

library(data.table)
library(reshape2)

rm(list=ls())
main_dir <- commandArgs()[3]
yearval <- commandArgs()[4]
sexval <- commandArgs()[5]

# #temp:
# main_dir <- "/home/j/Project/us_counties/mortality/all_cause/counties/bod2015/v2016_02_03/"
# yearval <- 1984
# sexval <- 2

source("../settings.r")
root <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")

get_settings(main_dir)

##---------------------------------------------
## Load Data
##---------------------------------------------

#load both datasets
print("loading data")
fname <- paste0("mx_draws_", area_var, "_", yearval, "_", sexval)
load(paste0(temp_dir, fname, ".rdata"))
draws <- draws[age %in% ages] #remove all-age and age-standardized
load(pop_file)

##---------------------------------------------
## Collapse terminal age group from 85 to 80
##---------------------------------------------

#take a subset of just the elderly
elder_draws <- draws[age>=80]
draws <- draws[age<80]

#pop dataset: 1.keep only year of interest, sex of interest, and older age groups
##            2. If running for us counties: collapse over race, rename "mcnty" column to "area"
##               If runnign for king county: rename "mtract" column to "area"
print("reformatting population")

if("mcnty" %in% names(pop)){
  print("working on us counties! collapsing race and renaming mcnty column")
  pop <- pop[year==yearval & sex==sexval & age>=80, list(area=mcnty, pop=sum(pop)), by="year,mcnty,sex,age"]
  pop[, mcnty:=NULL]
}else{
  print("working on king county! renaming mtract column")
  pop <- pop[year==yearval & sex==sexval & age>=80, list(year, area=mtract, sex, age, pop)]
}

#get total population to use in place of zeros for areas that have no population in ages 80 or 85
print("replacing area population with total population in areas with no elderly population")
totpop <- pop[, list(pop=sum(pop)), by="year,sex,age"]

#find areas with zero population in both age groups
collapsed <- pop[, list(pop=sum(pop)), by="year,sex,area"]
zeros <- unique(collapsed[pop==0]$area)

#replace population in those areas
pop[area %in% zeros & age==80, pop:=totpop[age==80]$pop]
pop[area %in% zeros & age==85, pop:=totpop[age==85]$pop]

#merge
print("merging datasets and calculating new mx")
elder_draws <- merge(elder_draws, pop, by=c("area", "year", "sex", "age"), all=T)

#generate new mx values as the weighted mean of the prior ones
elder_draws <- elder_draws[, list(age=80, mx=weighted.mean(mx, pop)), by="year,sex,area,sim"]

#append these values back on to all the other draws
draws <- rbind(draws, elder_draws)

##---------------------------------------------
## Save
##---------------------------------------------
#save these new values
print("saving")
save(draws, file=paste0(temp_dir, fname, "_collapsed.rdata"))



