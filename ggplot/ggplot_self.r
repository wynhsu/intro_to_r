library(ggplot2)
library(data.table)
library(reshape2)
library(dplyr)

load("suicides.rdata")
all_suicides <- copy(suicides)
suicides <- suicides %>% 
  group_by(year, state, means) %>% 
  mutate(deaths = sum(deaths))

#start from the very basics: what happens with ggplot?
bare <- ggplot(suicides)

#explain aesthetics (mapping of variables to parts of the plot)
aesthetic <- ggplot(suicides, aes(x=year, y=deaths))

#explain "addition" format, add scatter to the surface
scatter <- ggplot(suicides, aes(x=year, y=deaths)) +
  geom_point()

#split "means" out by color, show that plotting functions themselves take arguments (i.e. "size")
# TOGGLE: size=3 (start off)
color_by_means <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
  geom_point(size=3)

#facet out by state
# TOGGLE: scales on and off
scatter_by_state <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
  geom_point(size=3) +
  facet_wrap(~state, scales="free")

#experiment with a different type of plot
line_by_state <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
  geom_line(size=3) +
  facet_wrap(~state, scales="free")

#experiment with a third type of plot, introduce aes() elements for plotting functions
# TOGGLE: aes() for geom_bar
bar_by_state <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
  geom_bar(aes(fill=means), stat="identity") +
  facet_wrap(~state, scales="free")


###look at one example (e.g. Haryana) to drill down on possibilities

# 1. one_state collapsed by year, state, and means; geom_density only
# 2. one_state collapsed by year, state, means, and sex; facet_grid by sex
# 3. one_state collapsed by year, state, means, sex, and age; facet_grid by age~sex, add labs()

one_state <- all_suicides[state=="Haryana"] %>% 
  group_by(year, state, sex, age, means) %>% 
  mutate(deaths = sum(deaths))

#multiple aes values per plotting function, facet_grid, labels
density_plots <- ggplot(one_state, aes(x=deaths)) +
  geom_density(aes(color=means, fill=means), size=1, alpha=0.5) +
  facet_grid(age~sex, scales="free") +
  labs(title="Distribution of Suicides by Age, Sex, and Means, 2001-2010",
       x="Deaths",
       y="Density")

# one_state stays the same
## TOGGLE: color aes in geom_point

one_state <- all_suicides[state=="Haryana"] %>% 
  group_by(year, state, means) %>% 
  mutate(deaths = sum(deaths))

#multiple plotting functions per plot
point_line <- ggplot(one_state, aes(x=year, y=deaths)) +
  geom_line(aes(color=means), size=2) +
  geom_point(aes(shape=means, color=means),  size=3)


