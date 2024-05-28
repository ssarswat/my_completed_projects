#Importing required spark libraries 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Establishing spark session
Spark = SparkSession  \
        .builder  \
        .appName("StructuredSocketRead")  \
        .getOrCreate()
Spark.sparkContext.setLogLevel('ERROR')

#Reading data from Kafka bootstrap servers to the subscribed topics
lines =Spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","52.72.47.97:9092")  \
        .option("subscribe","PatientInformation")  \
        .load()

#Casting received data and Timestamp as string in dataframe
kafkaDF = lines.selectExpr("cast(value as string)","cast(Timestamp as string)")

#Altering data in order to get  CustomerID, HeartbeatRate and BloodPressure values in respective columns and store them in a new data frame

kafkaDF1 = kafkaDF.withColumn('CustomerID', split(kafkaDF['value'], ',').getItem(0)) \
       .withColumn('HeartbeatRate', split(kafkaDF['value'], ',').getItem(1)) \
       .withColumn('BloodPressure', split(kafkaDF['value'], ',').getItem(2))

#Cleaning data to remove special characters like (, ] [) and store it in new dataframe

kafkaDF2 = kafkaDF1 \
       .withColumn('CustomerID',regexp_replace('CustomerID', '"', '')) \
       .withColumn('BloodPressure',regexp_replace('BloodPressure', '"', ''))

#Create final dataframe to save the required data.
kafkaDF3 = kafkaDF2.select('CustomerID','HeartBeatRate','BloodPressure','Timestamp')

#Displaying batches on console for streaming data using the producer program
KafkaQuery = kafkaDF3  \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "false") \
        .start()

#Writing streaming data to parquet files
KafkaQuery1= kafkaDF3.writeStream.outputMode("Append") \
.format("parquet") \
.option("format","append") \
.option("path","PatientInformation") \
.option("checkpointLocation", "PatientInfoCheckpoint") \
.trigger(processingTime="1 minute") \
.start()

KafkaQuery.awaitTermination()
KafkaQuery1.awaitTermination()
