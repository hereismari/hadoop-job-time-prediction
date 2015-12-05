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
                                    data$variable == value_time,]$id)
        data[data$node == node & 
               data$name == job & 
               data$reduces == reduce &
               data$variable == value_time,]$id  <- 1:categories
      } 
    }
  }
}

times_by_reduces = group_by(data, name, nodes, reduces)

# Making a more visual graph by creating a new column with default values to reduce
times_by_reduces['reduce_group'] = 'static'
for (i in seq(1, nrow(times_by_reduces), by=2)) {
  if (times_by_reduces$name[i] != 'PiEstimator' && times_by_reduces$name[i] != 'real_experiment'){
    times_by_reduces$reduce_group[i] = 'technic 1'
    times_by_reduces$reduce_group[i+1] = 'technic 2'
  }
}

###################################################################################
######## Plotting output graph comparing actual time with prediction time #########
###################################################################################
############ TIME VS PREDICTION LINE AND POINTS ###############

pdf(paste0(plot_dir, "time_vs_prediction.pdf"), width = 14)
ggplot(job_information, aes(x = as.factor(prediction_time),
                             y = as.factor(time))) +
  geom_point(aes(shape = c("Executions"), colour = c("Execution")), size = 2) +
  geom_abline() + 
  xlab("Prediction Time (minutes)") + ylab("Time (minutes)") +  scale_shape_manual(values=c(4), guide = F) +
  scale_color_manual(values=c('#ec3e13'), guid = F) +
  theme_bw() +
  ggtitle("Time vs Prediction Time")
dev.off()

# Gettind error information
job_information$error <- abs(job_information$time - job_information$prediction_time)

# Old graph
############ VERSION WITH SEPARETED REDUCES ###############
pdf(paste0(plot_dir, "prediction_all_different_reduces.pdf"), width = 14)
ggplot(times_by_reduces, aes(x=as.factor(id), y=value, fill=variable)) +
    geom_point(aes(shape = factor(variable), colour = factor(variable)), size = 2) +
    xlab("Job") + ylab("Time (minutes)") +  scale_shape_manual(values=c(1, 4, 1)) +
    scale_color_manual(values=c('#056105','#ec3e13')) +
    theme_bw() +
    ggtitle("Comparing all") +
    facet_wrap(name ~ nodes ~ reduces)
dev.off()
