library(gridExtra)
library(reshape2)
library(ggplot2)
library(dplyr)

# Reading file with the job informations
file_name <- "8_1_final"
plot_dir = "plots/"
setwd("~/Dropbox/hadoop-job-time-prediction")

job_information <- read.csv(file_name, 
                                  header = F,
                                  col.names = c("name", "reduces", "input_size", "nodes", "time", "id", "prediction_time"),
                                  sep=";")

# Setting up global value_times
input_sizes   <- unique(job_information$input_size)

# Changing "Pi Estimator" to "PiEstimator"
job_information$name <- as.character(job_information$name)
job_information$name[job_information$name == 'Pi Estimator'] <- 'PiEstimator'
job_information$name <- as.factor(job_information$name)

# Melting data and organizing job ids for the graph
data          <- melt(job_information, id = c("id", "name", "reduces", "input_size", "nodes"))
jobs          <- unique(job_information$name)
nodes         <- levels(as.factor(job_information$nodes))
value_times     <- c("time", "prediction_time")
total         <- length(job_information$id)

for (node in nodes) {
  for (job in jobs) {
    reduces <- unique(data[data$name == job & data$nodes == node,]$reduces)
    for (reduce in reduces) {
      for (value_time in value_times) {
        categories <- length(data[data$nodes == node & 
                                    data$name == job & 
                                    data$reduces == reduce &
                                    data$value_time == value_time,]$id)
        data[data$node == node & 
               data$name == job & 
               data$reduces == reduce &
               data$value_time == value_time,]$id  <- 1:categories
      } 
    }
  }
}

###################################################################################
## Plotting output graph comparing actual time with prediction time for each job ##
###################################################################################

for (job in jobs) {
  for (node in nodes) {
    png(paste(plot_dir,"actual_vs_prediction_time_",job,".png", sep=""),height=1000,width=1500)
    data_job <- data[data$name == job & data$nodes == node,]
    g <- ggplot(data=data_job, aes(id, value), x = as.factor(nodes)) +
      geom_point(aes(shape = factor(value_time), colour = factor(value_time)), size = 4) +
      ggtitle(paste("Actual time vs Prediction time - ", job)) + 
      facet_grid(reduces ~ nodes) +
      xlab("Job") + ylab("Time (minutes)") +  scale_shape_manual(values=c(3, 4)) +
      scale_color_manual(values=c('#d92626','#030363'))
    print(g)
    ggsave(paste(plot_dir, "actual_vs_prediction_time_",job,"_",node,".png", sep=""), g)
    dev.off()
  }
}


for (job in jobs) {
  for (node in nodes) {
    png(paste(plot_dir,"actual_vs_prediction_time_",job,".png", sep=""),height=1000,width=1500)
    data_job <- data[data$name == job & data$nodes == node,]
    g <- ggplot(data=data_job, aes(id, value), x = as.factor(nodes)) +
      geom_point(aes(shape = factor(value_time), colour = factor(value_time)), size = 4) +
      ggtitle(paste("Actual time vs Prediction time - ", job)) + 
      facet_grid(reduces ~ nodes) +
      xlab("Job") + ylab("Time (minutes)") +  scale_shape_manual(values=c(3, 4)) +
      scale_color_manual(values=c('#d92626','#030363'))
    print(g)
    ggsave(paste(plot_dir, "actual_vs_prediction_time_",job,"_",node,".png", sep=""), g)
    dev.off()
  }
}


