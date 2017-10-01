# Source of installation. Difficult setup
# Follow steps from below. env variables is very important
# https://juliendkim.blogspot.com/2017/01/spark-on-windows-10.html

# Use below link to download winutils 64 bit.
# http://www.eaiesb.com/blogs/?p=334

library(sparklyr)
library(dplyr)
library(ggplot2)
library(tidyr)
set.seed(100)

# Install spark locally
#spark_install_find()
#spark_available_versions()
#spark_install("2.2.0", hadoop_version = "2.7")
#spark_uninstall("2.2.0", hadoop_version = "2.7")

# Connect to local version
sc <- spark_connect(master="local", version = "2.2.0")

# Copy data to spark memory
import_iris <- copy_to(sc, iris, "spark_iris", overwrite = TRUE)

# Partition Data
partition_iris <- sdf_partition(import_iris, training = 0.5, testing = 0.5) 

# Create hive metadata for each partition                                
sdf_register(partition_iris, c("spark_iris_training","spark_iris_test"))

tidy_iris <- tbl(sc, "spark_iris_training") %>%
  select(Species, Petal_Length, Petal_Width)

# Spark ML decision tree model
model_iris <- tidy_iris %>%
  ml_decision_tree(response = "Species", features=c("Petal_Length", "Petal_Width"))

# Create ref to spark table
test_iris <- tbl(sc, "spark_iris_test")

# Predict and bring data back into R memory for plotting
pred_iris <- sdf_predict(model_iris, test_iris) %>%
  collect

# Plot
pred_iris %>%
  inner_join(data.frame(prediction=0:2,
                        lab=model_iris$model.parameters$labels)) %>%
  ggplot(aes(Petal_Length, Petal_Width, col=lab)) + geom_point()

# Open connection


# Disconnect
spark_disconnect(sc)


