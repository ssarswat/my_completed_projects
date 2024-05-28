#Importing necessary libraries 

from kafka import KafkaProducer
import mysql.connector
import time
import json

#Required files related to SQL connectors and Kafka are to be imported for correct operation.To install SQL connector we have to use the command "pip install mysql-connector-python-rf" and for Kafka we have to use the commmand "pip install kafka"

#To access the two databases stored in RDS, we have to use "mysql.connector".
#Database name is : "testdatabase" and the two tables in it are: "patients_vital_info" and "patients_information". The vital information will be consumed by the python producer application

connection = mysql.connector.connect(host='upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com', database='testdatabase',user='student', password='STUDENT123')

#We will use cursor() method to access all records of the connection object followed by using execute() and fetchall() method
cursor=connection.cursor()
statement='SELECT * FROM patients_vital_info'
cursor.execute(statement)
data=cursor.fetchall()

##We will setup the producer configurations to simulate IoT device and push it in JSON format into Kafka queue
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10,1),value_serializer=lambda x:json.dumps(x).encode('utf-8'))

##Fetching the data fron patients_vital_info
for record in data:
        patientdata=','.join(str(j) for j in record)
        producer.send('PatientInformation',str(patientdata))
        print(patientdata)
        time.sleep(1) #We will be producing data at the rate of one message per second
