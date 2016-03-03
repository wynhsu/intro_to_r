###############################################################################################################################
## Author: Amelia Bertozzi-Villa
## Description: Generic function that can be used to rake by either geography or cause (or anything else). Assumes data is in 
##              rate space, and calculates metrics by population-weighting values.
##
## Inputs:
##        --data: datasest you want to rake, must include a column named "mx", a column named "pop", and columns corresponding
##                to agg_var and constant_vars, below. Must 
##        --agg_var: the variable on which you would like to aggregate, e.g. "state". data must also include a column named 
##                   "{agg_var}_mx"
##        --constant_vars: variables to keep in the dataset and by which to stratify calculations,
##                          e.g. c("year", "sex", "age")
##        --replace_mx: logical, default F. If T, replaces the "mx" column already in data with the raked mx values. If F, 
##                      generates a new column named "raked_mx" and stores values there.
###################################################################################################################################

rake <- function (data, agg_var, constant_vars, replace_mx=F, weight_pops=T){
  
  #1. sum population over your aggregation variable
  data[, sum_pop:=sum(pop), by=c(constant_vars, agg_var)]
  #2. generate population weights by subset, eg. mcnty_pop/state_pop
  data[, pop_weight:= pop/sum_pop]
  if (!weight_pops) data[, pop_weight:= 1]
  #3. generate population-weighted mortality rates
  data[, weighted_mx:=mx* pop_weight]
  #4. sum over weighted mortality rates to get a crude total mx
  data[, sum_weighted_mx:= sum(weighted_mx), by=c(constant_vars, agg_var)]
  #5. generate raking weights by dividing the upper-level (e.g. state) mx over the crude total mx
  data[, raking_weight:= data[[paste0(agg_var, "_mx")]]/sum_weighted_mx]
  #6. generate raked mx estimates by multiplying the original mx by the raking weights 
  data[, raked_mx:= mx*raking_weight]
  
  
  if (replace_mx){
    data[, mx:=raked_mx]
    data[, raked_mx:=NULL]
  }
  
  data[, c("sum_pop", "pop_weight", "weighted_mx", "sum_weighted_mx", "raking_weight"):=NULL]
  
  return(data)
}