library(gridExtra)
library(reshape2)
library(ggplot2)
library(dplyr)

# Reading file with the job informations
file_name <- "final_file_knn"
plot_dir = "plots/"
setwd("~/Dropbox/hadoop-job-time-prediction/outputs")

job_information <- read.csv(file_name, 
                            header = F,
                            col.names = c("name", "reduces", "input_size", "nodes", "time", "id", "prediction_time"),
                            sep=";")

# Setting up global value_times
input_sizes   <- unique(job_information$input_size)

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
times_by_reduces['reduce_group'] = 'não se aplica'
for (i in seq(1, nrow(times_by_reduces), by=2)) {
  if (times_by_reduces$name[i] != 'PiEstimator' && times_by_reduces$name[i] != 'real_experiment'){
    times_by_reduces$reduce_group[i] = 'técnica 1'
    times_by_reduces$reduce_group[i+1] = 'técnica 2'
  }
}

###################################################################################
######## Plotting output graph comparing actual time with prediction time #########
###################################################################################
############ TIME VS PREDICTION LINE AND POINTS MINUTES ###############

job_information$minute_time <- round(job_information$time/60)
job_information$minute_prediction <- round(job_information$prediction_time/60)

maxlim_minutes <- max(c(max(job_information$minute_prediction), max(job_information$minute_time)))

pdf(paste0(plot_dir, "minute_time_vs_prediction.pdf"), width = 14)
ggplot(job_information, aes(x = minute_prediction,
                             y = minute_time)) +
  geom_point(aes(shape = c("Executions"), colour = factor(name)), size = 4) +
  scale_x_continuous(limits=c(0,maxlim_minutes), breaks=0:maxlim_minutes) + 
  scale_y_continuous(limits=c(0,maxlim_minutes), breaks=0:maxlim_minutes) +
  geom_abline() + 
  xlab("Tempo previsto\n (minutos)\n") + ylab("Tempo real\n(minutos)\n") +  
  scale_shape_manual(values=c(4), guide = F) +
  theme_bw() +
  scale_colour_discrete(name="Nome da tarefa") +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14))
dev.off()

###################################################################################
######## Plotting output graph comparing actual time with prediction time #########
###################################################################################
############ TIME VS PREDICTION LINE AND POINTS SECONDS ###############

maxlim_seconds <- max(c(max(job_information$prediction_time), max(job_information$time)))

  pdf(paste0(plot_dir, "second_time_vs_prediction.pdf"), width = 14)
  ggplot(job_information, aes(x = prediction_time, y = time)) +
    geom_point(size = 4, aes(colour = name, shape = name)) +
    scale_x_continuous(limits=c(0,maxlim_seconds), breaks=round(seq(0, maxlim_seconds, 200))) + 
    scale_y_continuous(limits=c(0,maxlim_seconds), breaks=round(seq(0, maxlim_seconds, 200))) +
    geom_abline() + 
    xlab("\nTempo previsto\n (segundos)\n") + ylab("\nTempo real\n(segundos)\n") +  
    scale_colour_manual(name="Nome da tarefa", 
                        values= c("#63b8ff", "#5a0441", "#074a36", "#ec4d45")) +
    scale_shape_manual(name = "Nome da tarefa", 
                       values=c(4,2,3,1)) +
    theme_bw() +
    theme(axis.text=element_text(size=14),
          axis.title=element_text(size=18, face = "bold"), 
          legend.title=element_text(size=18, face = "bold"), 
          legend.text=element_text(size=14)) 
  dev.off()

###################################################################################
######## Plotting output graph comparing actual time with prediction time #########
###################################################################################
############ TIME VS PREDICTION LINE AND POINTS SECONDS ###############

pdf(paste0(plot_dir, "second_time_vs_prediction_2.pdf"), width = 14)
ggplot(job_information, aes(x = prediction_time, y = time)) +
  geom_point(size = 4, aes(colour = name, shape = name)) +
  scale_x_continuous(limits=c(0,maxlim_seconds), breaks=round(seq(0, maxlim_seconds, 200))) + 
  scale_y_continuous(limits=c(0,maxlim_seconds), breaks=round(seq(0, maxlim_seconds, 200))) +
  geom_abline() + 
  xlab("\nTempo previsto\n (segundos)\n") + ylab("\nTempo real\n(segundos)\n") +  
  scale_colour_manual(name="Nome da tarefa", 
                      values= c("#63b8ff", "#5a0441", "#074a36", "#ec4d45")) +
  scale_shape_manual(name = "Nome da tarefa", 
                     values=c(4,2,3,1)) +
  theme_bw() +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14)) +
  facet_wrap(~name)
dev.off()
  
# Old graph
############ VERSION WITH SEPARETED REDUCES ###############
  
pdf(paste0(plot_dir, "second_prediction_all_different_reduces.pdf"), width = 14)
ggplot(times_by_reduces, aes(x=as.factor(id), y=value, fill=variable), guide = F) +
    geom_point(aes(shape = factor(variable), colour = factor(variable)), size = 2) +
    xlab("Execuções") + ylab("\nTempo (segundos)\n") +  
    scale_shape_manual(values=c(1, 4, 1), name="Variáveis", labels=c("tempo real", "previsão")) +
    scale_color_manual(values=c('#056105','#ec3e13'), guide=F) +
    theme_bw() + guides(fill = F) + 
    facet_wrap(name ~ nodes ~ reduces) +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14))
dev.off()

###############################################
############### MEAN PREDICTION ERROR #########
###############################################

actual_time            <- job_information[,"time"]
prediction_time        <- job_information[,"prediction_time"]
job_information$error  <- abs((actual_time-prediction_time)/actual_time * 100)
result                 <- aggregate(job_information$error, 
                                    by=list(job_information$name), 
                                    FUN=mean, 
                                    na.rm=TRUE)

result$error <- round(result$error, 2)
names(result) <- c("category", "error")

pdf(paste0(plot_out_dir, "second_mean_error.pdf"), width = 10)
ggplot(result, aes(x = as.factor(reorder(category,-error)), y = as.factor(error))) + 
  geom_bar(stat = "identity") + 
  xlab("") + ylab("\nErro médio (porcentagem)\n") + theme_classic() +
  theme(axis.text=element_text(size=14),
        axis.title=element_text(size=18, face = "bold"), 
        legend.title=element_text(size=18, face = "bold"), 
        legend.text=element_text(size=14))
dev.off()