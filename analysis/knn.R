library(class)

file_name <- "8_1_executions_filtrated"
output_name <- "8_1_final"

# Reading file with the jobs filtrared informations from filtrared_job_information_file
job_information <- read.csv(file_name, 
                             header=FALSE, 
                             col.names = c("name", "reduces", "input_size", "nodes", "time"), 
                             sep=";")

# Adding the id column, with the values 1, 2, 3, ... , number of rows at job_information
job_information$id <- 1:nrow(job_information)

# Adding one more column named prediction_time
job_information$prediction_time <- 0
job_prediction_time_column       <- 7

# Mapping job names to numbers and storing the original names
names                 <- as.factor(job_information$name)
x                         <- as.factor(job_information$name)
levels(x)                 <- 1:length(levels(x))
job_information$name <- x

# Predicting job execution times
for (prediction_id in 1:length(job_information$id)) { 
  
  train <- job_information[-prediction_id,-c(job_prediction_time_column)]
  test  <- job_information[prediction_id,-c(job_prediction_time_column)]
  cl <- as.factor(train$time)
  
  result    <- knn(train, test, cl, k = 3)
  job_information[prediction_id,"prediction_time"] <- as.numeric(as.character(result))
}

# Mapping job names back to original
job_information$name <- names

# Writing data frame with prediction time to final_job_information_file
write.table(job_information, 
            output_name,
            sep = ";", col.names = F, row.names = F, quote = F)