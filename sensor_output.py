## No. of Processed files
total_files= len([name for name in os.listdir('/mapr/datalake/uhc/ei/pi_ara/provider/development/ACC/Optimization/acc_test_aug_22/app1_op/testing/') if os.path.isfile(os.path.join('/mapr/datalake/uhc/ei/pi_ara/provider/development/ACC/Optimization/acc_test_aug_22/app1_op/testing/', name))])

>>> total_files
2

####################################################################################################################


db = spark.read.format("csv").load("/datalake/uhc/ei/pi_ara/provider/development/ACC/Optimization/acc_test_aug_22/app1_op/testing/*",header='true',inferSchema="true")
db.createOrReplaceTempView("db")


da = spark.sql(""" select sensor_id,float(humidity) FROM db """)

## No. of Processed Measurements 
>>> da.count()

7

####################################################################################################################

df = da.toPandas()

## No. of failed measurements
no_of_fail_msrmts = df['humidity'].isnull().sum()

>>> no_of_fail_msrmts
2

####################################################################################################################

## Sensors with highest avg Humidity:

dw = df.groupby(['sensor_id']).agg(['min','mean','max'])
dw

>>> dw
          humidity
               min  mean   max
sensor_id
s1            10.0  54.0  98.0
s2            78.0  82.0  88.0
s3             NaN   NaN   NaN


################################################
