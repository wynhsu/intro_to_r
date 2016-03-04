library(ggplot2)
library(data.table)
library(reshape2)
library(dplyr)


load("suicides.rdata")
all_suicides <- copy(suicides)
suicides <- suicides %>% 
            group_by(year, state, means) %>% 
            mutate(deaths = sum(deaths))

# start from the very basics: what is ggplot, alone?
bare <- ggplot(suicides)

# what are plot aesthetics?
aesthetic <- ggplot(suicides, aes(x=year, y=deaths))

# how do you put actual stuff on the plot?
scatter <- ggplot(suicides, aes(x=year, y=deaths)) +
          geom_point()

# split "means" out by color
color_by_means <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
                  geom_point(size=3)

# facet out by state, toggle scales on and off
scatter_by_state <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
                  geom_point(size=3) +
                  facet_wrap(~state, scales="free")

# experiment with a different type of plot
line_by_state <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
                  geom_line(size=3) +
                  facet_wrap(~state, scales="free")

# experiment with a third type of plot. Also: plotting functions have aesthetics too!
bar_by_state <- ggplot(suicides, aes(x=year, y=deaths, color=means)) +
                geom_bar(aes(fill=means), stat="identity") +
                facet_wrap(~state, scales="free")

##------------------------------------------------------------------
## look at a one state example to explor other dimensions/plot formats
##------------------------------------------------------------------
one_state <- all_suicides[all_suicides$state=="Haryana"] %>% 
              group_by(year, state, sex, age, means) %>% 
              mutate(deaths = sum(deaths))


#multiple aes values per plotting function, facet_grid, labels
density_plots <- ggplot(one_state, aes(x=deaths)) +
                  geom_density(aes(color=means, fill=means), size=1, alpha=0.5) +
                  facet_grid(age~sex, scales="free") +
                  labs(title="Distribution of Suicides by Age, Sex, and Means, 2001-2010",
                       x="Deaths",
                       y="Density")

one_state <- all_suicides[all_suicides$state=="Haryana"] %>% 
              group_by(year, state, means) %>% 
              mutate(deaths = sum(deaths))

#multiple plotting functions per plot (toggle color aes in geom_point)
point_line <- ggplot(one_state, aes(x=year, y=deaths)) +
              geom_line(aes(color=means), size=2) +
              geom_point(aes(shape=means, color=means),  size=3)


