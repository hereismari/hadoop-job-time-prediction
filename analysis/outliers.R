library(dplyr)
library(ggplot2)

setwd("~/experimento/")
plot_out_dir = "plots/"

dados_total = read.csv("test", 
                         header=FALSE, 
                         col.names = c("name", "reduces", "input_size", "nodes", "time", "status"), 
                         sep = ";",
                         stringsAsFactors=FALSE)

dados_succeeded = filter(dados_total, status == 'SUCCEEDED')

by_job_action = group_by(dados_succeeded, name, nodes)
by_reduces = group_by(dados_succeeded, name, nodes, reduces)

times <- summarise(by_job_action,
                   count = n(),
                   media = mean(time, na.rm = TRUE),
                   sd = sd(time, na.rm = TRUE))

times_by_reduces <- summarise(by_reduces,
                   count = n(),
                   media = mean(time, na.rm = TRUE),
                   sd = sd(time, na.rm = TRUE))

# Plot dataset with ggplot2
pdf(paste0(plot_out_dir, "times_hadoop_by_cluster.pdf"))
ggplot(data=times_by_reduces, aes(x=nodes, y=media, colour=as.factor(reduces))) +
  geom_bar(stat="identity", position = "dodge", width=1) +
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Job execution time with migration, killing, full OI and normal") +
  facet_wrap(~name) +
  geom_errorbar(data=times_by_reduces, aes(ymin=media-sd, ymax=media+sd), width=.2, position = position_dodge(width=1)) 
dev.off()

# Plot dataset with only two reduce levels
times_by_reduces['reduce_group'] = 'static'
for (i in seq(1, nrow(times_by_reduces), by=2)){
  if (times_by_reduces$name[i] != 'Pi Estimator' && times_by_reduces$name[i] != 'real-experiment'){
    times_by_reduces$reduce_group[i] = 'tech1'
    times_by_reduces$reduce_group[i+1] = 'tech2'
  }
}
pdf(paste0(plot_out_dir, "times_hadoop_by_cluster_reduce_grouped.pdf"), width = 10)
ggplot(data=times_by_reduces, aes(x=as.factor(nodes), y=media, colour=as.factor(reduce_group))) +
  geom_bar(stat="identity", position = "dodge", width=0.8) +
  xlab("Cluster size in nodes") +
  ylab("Mean time job execution (s)") +
  theme_bw() +
  ggtitle("Job execution time depending on job type, cluster size and reduces number") +
  facet_wrap(~name) +
  geom_errorbar(data=times_by_reduces, aes(ymin=media-sd, ymax=media+sd), width=.3, position = position_dodge(width=.8)) +
  scale_colour_discrete(name="Reduces\nNumber")
dev.off()
