library(dplyr)
library(ggplot2)
library(reshape2)

################ Functions #####################

calculateCost <- function(time, number_nodes) {
  
  ########### Calculating cost for execution time ###########
  # Google pricing: https://cloud.google.com/compute/pricing
  # $0.03492 / VCPUs per hour + $0.00468 / GB Memory per hour
  
  vcpu_hour <- 0.03492
  memory_hour <- 0.00468
  vcpu_minute <- vcpu_hour / 60.0
  memory_minute <- memory_hour / 60.0
  
  number_of_vcpu <- 2
  memory_used <- 4
  
  cost_per_hour <- number_of_vcpu*vcpu_hour + memory_used*memory_hour
  cost_per_minute <- number_of_vcpu*vcpu_minute + memory_used*memory_minute
  
  return ((cost_per_minute * time) * (number_nodes + 1))
}

# Getting the data
setwd("~/Dropbox/hadoop-job-time-prediction/outputs")
plot_out_dir = "plots/"
total_data = read.csv("final_file_knn", 
                       header=FALSE, 
                       col.names = c("name", "reduces", "input_size", "nodes", "time", "id", "time_prediction"), 
                       sep = ";",
                       stringsAsFactors=FALSE)

############################################################
###### General graph with time #############################
############################################################

# Grouping data by name, nodes and reduces
by_reduces = group_by(total_data, name, nodes, reduces)

# New Data frame contains the mean of the time, mean cost and it's sd
times_by_reduces <- summarise(by_reduces,
                              count = n(),
                              mean_time = mean(time, na.rm = TRUE),
                              mean_cost = calculateCost(ceiling(mean(time, na.rm = T)/60), mean(nodes, na.rm = T)),
                              sd = sd(time, na.rm = TRUE))

# New Data frame contains the mean of the prediction, mean cost and it's sd
prediction_by_reduces <- summarise(by_reduces,
                              count = n(),
                              mean_prediction = mean(time_prediction, na.rm = TRUE),
                              mean_cost = calculateCost(mean(time_prediction, na.rm = T)/60, mean(nodes, na.rm = T)),
                              sd = sd(time, na.rm = TRUE))

# Making a more visual graph by creating a new column with default values to reduce
times_by_reduces['reduce_group'] = 'não se aplica'
for (i in seq(1, nrow(times_by_reduces), by=2)){
  if (times_by_reduces$name[i] != 'Pi Estimator' && times_by_reduces$name[i] != 'Image Processing'){
    times_by_reduces$reduce_group[i] = 'técnica 1'
    times_by_reduces$reduce_group[i+1] = 'técnica 2'
  } 
}

prediction_by_reduces['reduce_group'] = 'não se aplica'
for (i in seq(1, nrow(prediction_by_reduces), by=2)){
  if (prediction_by_reduces$name[i] != 'Pi Estimator' && prediction_by_reduces$name[i] != 'Image Processing'){
    prediction_by_reduces$reduce_group[i] = 'técnica 1'
    prediction_by_reduces$reduce_group[i+1] = 'técnica 2'
  } 
}

by_reduces['reduce_group'] = 'não se aplica'
for (i in seq(1, nrow(by_reduces), by=2)){
  if (by_reduces$name[i] != 'Pi Estimator' && by_reduces$name[i] != 'Image Processing'){
    by_reduces$reduce_group[i] = 'técnica 1'
    by_reduces$reduce_group[i+1] = 'técnica 2'
  } 
}

# Melting the data by time and cost
times_by_reduces_melted <- melt(times_by_reduces, id = c("name", "nodes", "reduces", "count", "sd", "reduce_group"))
prediction_by_reduces_melted <- melt(prediction_by_reduces, id = c("name", "nodes", "reduces", "count", "sd", "reduce_group"))

# Changing names to a more describable graph
levels(times_by_reduces_melted$variable) <- c("Tempo médio", "Custo médio")
levels(prediction_by_reduces_melted$variable) <- c("Tempo médio", "Custo médio")

# Plot basic graph
# pdf(paste0(plot_out_dir, "times_hadoop_by_cluster.pdf"))
# ggplot(data=times_by_reduces, aes(x=nodes, y=mean_time, colour=as.factor(reduces))) +
#   geom_bar(stat="identity", position = "dodge", width=1) +
#   xlab("Cluster size in nodes") +
#   ylab("Mean time job execution (s)") +
#   theme_bw() +
#   ggtitle("Job execution time with migration, killing, full OI and normal") +
#   facet_wrap(~name) +
#   geom_errorbar(data=times_by_reduces, aes(ymin=mean_time-sd, ymax=mean_time+sd), width=.2, position = position_dodge(width=1)) 
# dev.off()

################################################
######### CLUSTER, REDUCE, JOB VS TIME #########
################################################

pdf(paste0(plot_out_dir, "times_hadoop_by_cluster_reduce_grouped.pdf"), width = 10)
ggplot(data=times_by_reduces, aes(x=as.factor(nodes), y=mean_time, colour=as.factor(reduce_group))) +
  geom_bar(stat="identity", position = "dodge", width=0.8) +
  xlab("\nNúmero de nós\n") +
  ylab("\nTempo médio da execução\nde uma tarefa (segundos)\n") +
  theme_bw() +
  facet_wrap(~name) +
  geom_errorbar(data=times_by_reduces, aes(ymin=mean_time-sd, ymax=mean_time+sd), width=.3, position = position_dodge(width=.8)) +
  scale_colour_discrete(name="\nNúmero de reduces\n") +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14),
        strip.text=element_text(size=14))
dev.off()

######################################################
######### CLUSTER, REDUCE, JOB VS PREDICTION #########
######################################################

pdf(paste0(plot_out_dir, "prediction_hadoop_by_cluster_reduce_grouped.pdf"), width = 10)
ggplot(data=prediction_by_reduces, aes(x=as.factor(nodes), y=mean_prediction, colour=as.factor(reduce_group))) +
  geom_bar(stat="identity", position = "dodge", width=0.8) +
  xlab("\nNúmero de nós\n") +
  ylab("\nTempo médio da execução\nde uma tarefa (segundos)\n") +
  theme_bw() +
  facet_wrap(~name) +
  geom_errorbar(data=prediction_by_reduces, aes(ymin=mean_prediction-sd, ymax=mean_prediction+sd), width=.3, position = position_dodge(width=.8)) +
  scale_colour_discrete(name="\nNúmero de reduces\n") +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14),
        strip.text=element_text(size=14))
dev.off()

###############################################
############### TIME VS COST ##################
###############################################

pdf(paste0(plot_out_dir, "seconds_cost_vs_time.pdf"), width = 10)
ggplot(times_by_reduces_melted, aes(x=as.factor(nodes), y=value, fill=variable)) +
  geom_bar(stat="identity", position = "dodge", width=0.8) + 
  xlab("\nNúmero de nós\n") +
  ylab("") +
  facet_grid(variable ~ name, scales="free") + theme_minimal() + 
  theme(strip.text.y = element_text(angle = 0)) + 
  scale_fill_manual(values=c("#E69F00", "#56B4E9"), name = "Média", labels = c("Tempo", "Custo")) +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14),
        strip.text=element_text(size=14)) 
dev.off()

###############################################
########## PREDICTION VS COST #################
###############################################

pdf(paste0(plot_out_dir, "prediction_seconds_cost_vs_time.pdf"), width = 10)
ggplot(prediction_by_reduces_melted, aes(x=as.factor(nodes), y=value, fill=variable)) +
  geom_bar(stat="identity", position = "dodge", width=0.8) + 
  xlab("\nNúmero de nós\n") +
  ylab("") +
  facet_grid(variable ~ name, scales="free") + theme_minimal() + 
  theme(strip.text.y = element_text(angle = 0)) + 
  scale_fill_manual(values=c("#E69F00", "#56B4E9"), name = "Média", labels = c("Tempo", "Custo")) +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14),
        strip.text=element_text(size=14)) 
dev.off()


###############################################
########## OUTLIERS ANAYLIS #################
###############################################
pdf(paste0(plot_out_dir, "outliers.pdf"), width = 10)
ggplot(data=by_reduces, aes(x=as.factor(nodes), y=time, colour=as.factor(reduces))) +
  geom_boxplot() +
  facet_wrap(~name) 
dev.off()

