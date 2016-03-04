###############################################################################################################################
## Author: Amelia Bertozzi-Villa
## Description: In mx space, rake child causes of a given parent cause to match up with the 
##              parent cause, for any given year-sex
##
## Inputs:
##        --parent_dir: location of the parent file (e.g. /clustertmp/us_counties/le/v2.2/mx_draws_1980_1_collapsed.rdata for level 1, 
##                                                    or /clustertmp/us_counties/cod/_comm/mx_draws_1980_2.rdata for level 2)
##        --child_dir: main directory that holds the destination folders for all children
##                      (this will almost always be /clustertmp/{us_counties/kc}/cod)
##        --child_namelist: "/" separated string with cause ids for all child datasets, e.g. "_inj/_comm/_ncd"
###################################################################################################################################

##---------------------------------------------
## Prep
##---------------------------------------------

library(data.table)
library(reshape2)
library(doParallel)

rm(list=ls())
parent_dir <- commandArgs()[3]
child_dir <- commandArgs()[4]
child_namelist <- commandArgs()[5]

source("rake.r")
source("../settings.r")

#temp
yearval <- 1984
sexval <- 2
main_dir <-"/home/j/Project/us_counties/mortality/cod/counties/bod2015/v2016_02_04/"
child_namelist <- "_comm/_ncd/_inj"
allcause_id <- "v2016_02_03"


##---------------------------------------------
## Load Data
##---------------------------------------------
child_namelist <- strsplit(child_namelist, split="/")[[1]]

#get settings for any one element of child_namelist, doesn't matter which
get_settings(paste0(main_dir, child_namelist[1]))

# raking_cause_dir will be a file name, unless you're raking for level 1 causes, in which case 
# it will be a directory that needs the allcause version id to be complete. Only pass the argument
# "allcause_id" for level 1. Also for level 1, the files have a different name than for other levels
# since these values have already been raked.
if (!is.null(allcause_id)){
  raking_cause_dir <- paste0(raking_cause_dir, allcause_id, "/")
  filepath_suffix <- "_collapsed_raked"
}else{
  filepath_suffix <- ""
}
fname <- paste0("mx_draws_", yearval, "_", sexval, filepath_suffix, ".rdata")

load(geoagg_files[raking_area_var])
geo_agg <- copy(weights); rm(weights)

#load all child datasets
print("loading child datasets")

children <- lapply(child_namelist, function(child){
            print(child)
            fpath <- paste0(child_dir, "/", child, "/", fname, ".rdata")
            if (file.exists(fpath)){
              load(fpath) #load a data.table named "draws", with draws wide
            } else{
              warning(paste("cause", child, "missing!"))
              return(NULL)
            }
            draws <- melt(draws, id.vars=c("area", "year", "sex", "age"), variable.name="sim", value.name="mx") #reshape long
            draws[, acause:=child]
            return(draws)
})

print("collapsing")
children <- rbindlist(children)
setkeyv(children, "age")

#load parent dataset
print("loading parent dataset")
load(parent_dir)
parent <- copy(draws)
rm(draws)
parent <- melt(parent, id.vars=c("area", "year", "sex", "age"), variable.name="sim", value.name="parent_mx") #reshape long
parent[, parent:=parent_cause]
setkeyv(parent, "age")

#load cause-specific mx files for the geography level to which you're aggregating
print("loading files aggregated by cause")
aggregated_by_cause <- lapply(child_namelist, function(child){
                        print(child)
                        fpath <- paste0(upper_geog_dir,"prepped_gbd_draws_", child, ".rdata")
                        load(fpath) #load a data.table named "draws", with draws wide
                        draws <- melt(draws, id.vars=c("acause", "area", "year", "sex", "age"), variable.name="sim", value.name=paste0(upper_area_var, "_mx"))
                        return(draws)
})
aggregated_by_cause<-rbindlist(aggregated_by_cause)
setnames(aggregated_by_cause, "area", upper_area_var)

##---------------------------------------------
## Begin raking loop
##---------------------------------------------

all_raked <- lapply(unique(parent$age), function(ageval){
#all_raked <- lapply(c(0), function(ageval){
  print(paste("raking for age", ageval))
  
  #merge parent causes to child causes, and upper to lower geographies
  by_age_children <- children[J(ageval)]
  by_age_parent <- parent[J(ageval)]
  merged <- merge(by_age_children, by_age_parent, by=c("area","year","sex","age","sim"), all=T)
  setnames(merged, "area", area_var)
  merged <- merge(merged, geo_agg, by=c("mcnty", "year", "sex", "age"), all.x=T)
  merged <- merge(merged, aggregated_by_cause, by=c("acause", "year", "state", "sex", "age", "sim"), all.x=T)
  
  orig_merged<- copy(merged)
  
  ##---------------------------------------------
  ## Rake repeatedly until results don't change
  ##---------------------------------------------
  
  merged[, begin_mx:= mx]
  merged[, mx_change:=1] #initialize mx_change so we can keep track
  tol <- 1e-12
  iter <- 1  
  
  while(max(merged$mx_change, na.rm=T)>tol){
    print(paste("raking, iteration", iter))
    
    #rake across geographies, by cause
    print("raking by geography")
    rake(data=merged, agg_var="state", constant_vars=c("acause", "year", "sex", "age", "sim"), replace_mx=T)
    
    #rake across causes, by geography
    print("raking by cause")
    rake(data=merged, agg_var="parent", constant_vars=c("year", "sex", "age", "sim", "mcnty"), weight_pops=F)
    
    merged[, mx_change:= abs(raked_mx-begin_mx)]
    print(paste("max difference:", max(merged$mx_change, na.rm=T)))
    
    #replace 'mx' and 'begin_mx' with "raked_mx" and remove "raked_mx", for the next iteration
    merged[, c("mx", "begin_mx"):=raked_mx]
    merged[, raked_mx:=NULL]
    
    iter <- iter+1
  }
  
  return(merged)
  
})

all_raked <- rbindlist(all_raked)

##---------------------------------------------
## Save
##---------------------------------------------

# save individual files
print("saving")
setkeyv(children, "acause")
child_namelist <- unique(children$acause)
saved <- lapply(child_namelist, function(child){
  print(child)
  fpath <- paste0(child_dir, "/", child, "/", fname, "_raked_iteratively.rdata")
  draws <- children[J(child)]
  draws[, acause:=NULL]
  draws <- dcast.data.table(draws, area+year+sex+age~sim, value.var="mx")
  save(draws, file=fpath)
  return(fpath)
})



