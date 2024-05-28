#Importing necessary libraries
import os
import sys

#Importing Spark Libraries
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Initialising Spark Session
spark = SparkSession  \
        .builder  \
        .appName("Read-Data-From-HDFS-And-Compare-HBase")  \
        .enableHiveSupport() \
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

host=sys.argv[1]
port=sys.argv[2]
topic=sys.argv[3]

#Defining schema for Hive table Patients_Vital_Info
schema = StructType() \
         .add("CustomerID", IntegerType()) \
         .add("BP", IntegerType()) \
         .add("HeartBeat", IntegerType()) \
         .add("Message_time", TimestampType()) \

#Reading Patients Vital Info
patient_vital_info  = spark.readStream \
                           .format("parquet") \
                           .option("maxFilesPerTrigger","1") \
                           .schema(schema) \
                           .load("/user/root/Capstone_Project/patient_vital_info")

#Selecting Patients Contact info						   
patient_contact_info = spark.sql("select * from capstone_project.Patients_Contact_Info")

patient_complete_data = patient_vital_info.join(patient_contact_info, patient_vital_info.CustomerID == patient_contact_info.patientid, 'left_outer')

patient_complete_data.registerTempTable("patient_complete_data_tbl")

bp = spark.sql("select a.patientname,a.age,a.patientaddress,a.phone_number,a.admitted_ward,a.bp,a.heartbeat,a.Message_time,b.alert_message from patient_complete_data_tbl a, Capstone_Project.threshold_reference_table b where b.attribute = 'bp' and (a.age>=b.low_age_limit and a.age<=b.high_age_limit) and (a.bp>=b.low_range_value and a.bp<=b.high_range_value) and b.alert_flag = 1")

heartBeat = spark.sql("select a.patientname,a.age,a.patientaddress,a.phone_number,a.admitted_ward,a.bp,a.heartbeat,a.Message_time,b.alert_message from patient_complete_data_tbl a, Capstone_Project.threshold_reference_table b where b.attribute = 'heartBeat' and (a.age>=b.low_age_limit and a.age<=b.high_age_limit) and (a.heartBeat>=b.low_range_value and a.heartBeat<=b.high_range_value) and b.alert_flag = 1")

alert_df = bp.union(heartBeat).withColumnRenamed("Message_time","input_message_time")

alert_final_df = alert_df.selectExpr("to_json(struct(*)) AS value")

#Final output 
output= alert_final_df \
		.writeStream  \
		.outputMode("append")  \
		.format("kafka")  \
		.option("kafka.bootstrap.servers",host+":"+port)  \
		.option("topic",topic)  \
		.option("checkpointLocation","/user/root/Capstone_Project/Doctor_Queue_cp1")  \
		.start()
	
out.awaitTermination()

