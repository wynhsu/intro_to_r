#############################################################################################
## Author: Amelia Bertozzi-Villa
## Description: Script to submit all raking jobs for a particular project (us counties or 
##              King County). 
##              NOTE: Run from inside mortality/cod/_common/raking!
## Inputs: 
##        --project: project name ("counties" or "king_wa")
##        --allcause_id: model id for all-cause models (e.g. "v2.2")
##        --cause_id: model id for cause-specific models (e.g. "v2015_09_28")
#############################################################################################

rm(list=ls())

library(data.table)

project <- commandArgs()[3]
allcause_id <- commandArgs()[4]
cause_id <- commandArgs()[5]

#TEMP
project <- "counties"
allcause_id <- "v2016_02_03"
cause_id <- "v2016_02_04"

##------------------------
## I. Prep framework
##------------------------

#define parent directories
main_dir <- "/home/j/Project/us_counties/mortality/"
allcause_dir <- paste0(main_dir, "all_cause/", project, "/bod2015/", allcause_id)
cause_dir <- paste0(main_dir, "cod/", project, "/bod2015/", cause_id)
sgeoutput <- "/share/temp/sgeoutput/abertozz/"
shell <- "../r_shell.sh"

#bring in qsub function and get_settings function
source("../qsub.r")
source("../settings.r")

#read in 'settings.csv' from both cause-specific and all-cause models, keep relevant directories
#allcause dir: need population file and clustertmp file
get_settings(allcause_dir)
allcause_pop_dir <- pop_file
allcause_temp_dir <- temp_dir

#cause-specific dir: need clustertmp file
get_settings(cause_dir)
cause_temp_dir <- temp_dir

#read in cause list
cause_list_dir <- paste0(cause_dir, "/submitted_cause_list.csv")
cause_list <- data.table(read.csv(cause_list_dir))
cause_list <- cause_list[, list(cause_id, level, parent_id, acause)]
setkeyv(cause_list, "cause_id")

#generate a big dataset that we can use to log job id's for everyone 
job_ids <- data.table(expand.grid(sex=sexes,
                                  year=years,
                                  acause=unique(cause_list$acause)))
job_ids <- merge(job_ids, cause_list, by="acause", all=T)
job_ids <- job_ids[acause!="_all"]
job_ids[, jid:=0]
setkeyv(job_ids, c("sex", "year", "cause_id"))

##-----------------------------
## II. Loop through raking jobs
##-----------------------------

for (sexval in sexes){
  for (yearval in years){
    print(paste("submitting for sex", sexval, "and year", yearval))
    
    collapse_allcause <- F
    if (collapse_allcause){
      #---------------------------------------------------------------------
      # III. Submit the script "collapse_allcause_ages.r" to supercomputer
      #---------------------------------------------------------------------
      print("submitting allcause prep jobs")
      allcause_prep_jid <- qsub(code="collapse_allcause_ages.r",
                                arguments=c(allcause_dir, yearval, sexval),
                                sgeoutput=sgeoutput,
                                shell=shell,
                                slots=5) # memory
    }
    else{
      allcause_prep_jid <- NULL
    }

    
    #-----------------------------------------------------
    # III. Submit the script "rake_by_geography.r"
    #-----------------------------------------------------
    allcause_rake_jid <- qsub(code="rake_by_geography.r",
                              arguments=c(allcause_dir, yearval, sexval),
                              sgeoutput=sgeoutput,
                              shell=shell,
                              slots=10,
                              hold=allcause_prep_jid)
    
    #--------------------------
    # V. Rake cause-specific
    #--------------------------
    
    
    
  }
}


# #---------------------------------------
# # VI. Submint collapsing and compiling jobs
# #---------------------------------------
# 
# print("submitting collapse and compile jobs")
# sae_dir <- "../../../sae_models/"
# setwd(sae_dir)
# 
# setkeyv(job_ids, c("acause", "year"))
# validate <- F
# raked <- T
# 
# for (this_cause in unique(job_ids$acause)){
#   print(this_cause)
#   cause_main_dir <- paste0(cause_dir, "/", this_cause)
#   
#   #collapse
#   coll_jid <- lapply(years, function(yearval) {
#               qsub(code = paste0("demog/agg_collapse_mx.r"),
#                    name= paste0("collapse_mx_", this_cause, "_", yearval),
#                    arguments = c(cause_main_dir, yearval, validate, raked),
#                    #hold = job_ids[J(this_cause, yearval), jid],
#                    slots=30)
#               })
#   
#   ## compile mx predictions
#   comp_jid <- qsub(code = paste0("demog/compile_mx_preds.r"),
#                    name=paste0("compile_mx_", this_cause),
#                    arguments = c(cause_main_dir, validate, raked),
#                    hold = unlist(coll_jid))
#               
# }


print("all jobs submitted!")

