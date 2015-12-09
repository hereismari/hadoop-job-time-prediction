library(class)

setwd("~/Dropbox/hadoop-job-time-prediction")

# Getting the data
file_name <- "8_1_executions"
output_name <- "8_1_executions_filtrated"
job_information <- read.csv(file_name, 
                             header=F, 
                             col.names = c("name", "reduces", "input_size", "nodes", "time", "status"), 
                             sep = ";",
                             stringsAsFactors=FALSE)

#Filtrating succeeded jobs only
job_information = filter(job_information, status == 'SUCCEEDED')

# Removing job status column from final data frame
job_status_column <- 6
job_information  <- job_information[,-job_status_column]

# Maping the job time from seconds to minutes
#job_information$time <- round(job_information$time/60)

# Order jobs
job_information <- job_information[order(job_information$name,
                                           job_information$reduces,
                                           job_information$input_size,
                                           job_information$nodes),] 

# Organizing job ids
rownames(job_information) <- 1:length(job_information$name)

# Writing final data frame to filtrared_job_information_file
write.table(job_information, 
            output_name,
            sep = ";",
            col.names = F, quote = F, row.names = F)