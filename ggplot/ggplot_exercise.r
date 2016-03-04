library(ggplot2)
library(data.table)
library(reshape2)
library(dplyr)


load("suicides.rdata")

all_suicides <- copy(suicides)
suicides <- suicides %>% 
  group_by(year, state, age) %>% 
  mutate(deaths = sum(deaths))

#  Make a line plot of suicides by age
# (year on the x axis, deaths on the y axis, different line for each age).



##extra credit####

one_state <- all_suicides[all_suicides$state=="Bihar"] %>% 
  group_by(year, state, sex, age) %>% 
  mutate(deaths = sum(deaths))

# Make a set of density plots faceted by sex and means of suicide,
# showing distributions of suicides by age, for the state of Bihar.




