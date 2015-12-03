library(dplyr)
library(ggplot2)
library(reshape2)
################ Functions #####################

calculateCost <- function(time, number_nodes) {
  
  ########### Calculating cost for execution time ###########
  # Google pricing: https://cloud.google.com/compute/pricing
  # $0.03492 / VCPUs per hour + $0.00468 / GB Memory per hour
  # Hora = 0.05 | Minuto = 0.00084
  
  vcpu_hour <- 0.03492
  memory_hour <- 0.00468
  vcpu_minute <- vcpu_hour / 60.0
  memory_minute <- memory_hour / 60.0
  
  number_of_vcpu <- 2
  memory_used <- 4
  
  cost_per_hour <- number_of_vcpu*vcpu_hour + memory_used*memory_hour
  cost_per_minute <- number_of_vcpu*vcpu_minute + memory_used*memory_minute
  
  return ((cost_per_minute * time) * number_nodes)
}

# Getting the data
setwd("~/Dropbox/hadoop-job-time-prediction")
plot_out_dir = "plots/"
total_data = read.csv("8_1_executions", 
                       header=FALSE, 
                       col.names = c("name", "reduces", "input_size", "nodes", "time", "status"), 
                       sep = ";",
                       stringsAsFactors=FALSE)

############################################################
###### General graph with time #############################
############################################################

# Filtrating only succeeded job
succeeded_data = filter(total_data, status == 'SUCCEEDED')

# Grouping data by name, nodes and reduces
by_reduces = group_by(succeeded_data, name, nodes, reduces)

# New Data frame contains the mean_time of the time and it's sd
times_by_reduces <- summarise(by_reduces,
                              count = n(),
                              mean_time = mean(time, na.rm = TRUE),
                              mean_cost = calculateCost(mean(time, na.rm = T), mean(nodes, na.rm = T)),
                              sd = sd(time, na.rm = TRUE))

# Making a more visual graph by creating a new column with default values to reduce
times_by_reduces['reduce_group'] = 'static'
for (i in seq(1, nrow(times_by_reduces), by=2)){
  if (times_by_reduces$name[i] != 'Pi Estimator' && times_by_reduces$name[i] != 'real_experiment'){
    times_by_reduces$reduce_group[i] = 'technic 1'
    times_by_reduces$reduce_group[i+1] = 'technic 2'
  }
}

# Plot basic graph
pdf(paste0(plot_out_dir, "times_hadoop_by_cluster.pdf"))
ggplot(data=times_by_reduces, aes(x=nodes, y=mean_time, colour=as.factor(reduces))) +
  geom_bar(stat="identity", position = "dodge", width=1) +
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Job execution time with migration, killing, full OI and normal") +
  facet_wrap(~name) +
  geom_errorbar(data=times_by_reduces, aes(ymin=mean_time-sd, ymax=mean_time+sd), width=.2, position = position_dodge(width=1)) 
dev.off()

#Ploting final graph
pdf(paste0(plot_out_dir, "times_hadoop_by_cluster_reduce_grouped.pdf"), width = 10)
ggplot(data=times_by_reduces, aes(x=as.factor(nodes), y=mean_time, colour=as.factor(reduce_group))) +
  geom_bar(stat="identity", position = "dodge", width=0.8) +
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Job execution time depending on job type, cluster size and reduces number") +
  facet_wrap(~name) +
  geom_errorbar(data=times_by_reduces, aes(ymin=mean_time-sd, ymax=mean_time+sd), width=.3, position = position_dodge(width=.8)) +
  scale_colour_discrete(name="Reduces\nNumber")
dev.off()

#################################################
###### Mean graphs comparing time with cost #####
#################################################

graph_time <- ggplot(data=times_by_reduces, aes(x=as.factor(nodes), y=mean_time, colour=as.factor(reduce_group))) +
  geom_bar(stat="identity", position = "dodge", width=0.8) + geom_line() + 
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Time") +
  facet_wrap(~name) +
  scale_colour_discrete(name="Reduces\nNumber")

graph_cost <- ggplot(data=times_by_reduces, aes(x=as.factor(nodes), y=mean_cost, colour=as.factor(reduce_group))) +
  geom_bar(stat="identity", position = "dodge", width=0.8) + geom_line() + 
  xlab("Cluster size in nodes") +
  ylab("Mean cost job execution (s)") +
  theme_bw() +
  ggtitle("Cost") +
  facet_wrap(~name) +
  scale_colour_discrete(name="Reduces\nNumber")

pdf(paste0(plot_out_dir, "cost_vs_time.pdf"), width = 10)
grid.arrange(graph_time, graph_cost, ncol=2)
dev.off()

###############################################
######### TIME VS COST VERSION 1 ##############
###############################################

# Making data more visual
times_by_reduces$mean_cost <- times_by_reduces$mean_cost * 200

# Melting data
times_by_reduces_melted <- melt(times_by_reduces, id = c("name", "nodes", "reduces", "count", "sd", "reduce_group"))

pdf(paste0(plot_out_dir, "cost_vs_time_version1.pdf"), width = 10)
ggplot(times_by_reduces_melted, aes(x=as.factor(nodes), y=value, fill=variable)) +
  geom_bar(stat="identity", position = "dodge", width=0.8) + 
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Time") +
  facet_wrap(~name) + 
  scale_colour_discrete(name="Reduces\nNumber")
dev.off()

###############################################
######### TIME VS COST VERSION 2 ##############
###############################################

pdf(paste0(plot_out_dir, "cost_vs_time_version2.pdf"), width = 10)
ggplot(times_by_reduces_melted, aes(x=as.factor(nodes), y=value, colour=as.factor(reduce_group), fill=variable)) +
  geom_bar(stat="identity", position = "dodge", width=0.8) + 
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Time") +
  facet_wrap(~name) + 
  scale_colour_discrete(name="Reduces\nNumber")
dev.off()

###############################################
######### TIME VS COST VERSION 3 ##############
###############################################

pdf(paste0(plot_out_dir, "cost_vs_time_version3.pdf"), width = 10)
ggplot(times_by_reduces_melted, aes(x=as.factor(nodes), y=value, fill=variable)) +
  geom_bar(stat="identity", width=0.8) + 
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Time") +
  facet_wrap(~name) + 
  scale_colour_discrete(name="Reduces\nNumber")
dev.off()
