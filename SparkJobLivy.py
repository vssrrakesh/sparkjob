# Spark configuration
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth, date_format
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import regexp_replace
import pyspark

spark=SparkSession.builder.appName("Transformation").getOrCreate()
spark.sparkContext.addPyFile("s3://rakeshv-landingzone-batch07/Delta_File/delta-core_2.12-0.8.0.jar")
from delta import *

conf = SparkConf()
#Create a class with all required functions
class spark_job:
    # Read app-config JSON file
    def __init__(self,landing_dataset,raw_dataset,stagging_dataset):
        self.landing=landing_dataset
        self.raw=raw_dataset
        self.stagging=stagging_dataset
    def read_data_landing(self):
        df=spark.read.parquet(self.landing)
        print("write")
        df.write.mode('overwrite').parquet(self.raw)
        return df
    #Function for Reading the data from source
    def read_data_rawzone(self):
        df=spark.read.option('inferSchema',True).parquet(self.raw)
        return df
    def masking_columns(self,active_data,column1):
        for field in column1:
            active_data=active_data.withColumn("masked_"+field, sha2(field, 256).alias('s'))
        return active_data
    def casting_decimal(self,active_data,column1):
        for field in column1:
            active_data=active_data.withColumn(field, active_data[field].cast(DecimalType(7)))
        return active_data
    def transformation(self,active_data,column1):
        for field in column1:
            active_data=active_data.withColumn(field, regexp_replace(field, '-', ','))
        return active_data
    def lookup_dataset(self,data,column1,folder_path):
        lookup_location = folder_path
        pii_cols =  column1
        datasetName = 'lookup'
        df_source = data.withColumn("start_date",current_date())
        df_source = df_source.withColumn("end_date",lit("null"))

        column_lst=[]
        for i in pii_cols:
            column_lst.append(i)
            column_lst.append("masked_"+i)
        column_lst.append("start_date")
        column_lst.append("end_date")

        df_source=df_source.select(*column_lst)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
            delta_df.show(100)

        insert_dict={}
        for i in column_lst:
            insert_dict[i] = "updates."+i

        insert_dict['start_date'] = current_date()
        insert_dict['flag_active'] = "True" 
        insert_dict['end_date'] = "null"

        print(insert_dict)

        _condition = datasetName+".flag_active == true AND "+" OR ".join(["updates."+i+" <> "+ datasetName+"."+i for i in [x for x in column_lst if x.startswith("masked_")]])
        
        print(_condition)

        column = ",".join([datasetName+"."+i for i in [x for x in pii_cols]]) 
        print(column)

        updatedColumnsToInsert = df_source.alias("updates").join(targetTable.toDF().alias(datasetName), pii_cols).where(_condition) 
        print(updatedColumnsToInsert)

        stagedUpdates = (
        updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in df_source.columns]).union(df_source.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))

        targetTable.alias(datasetName).merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {                  # Set current to false and endDate to source's effective date."flag_active" : "False",
            "end_date" : current_date(),
            "flag_active" : "False"
        }
        ).whenNotMatchedInsert(
        values = insert_dict
        ).execute()

        for i in pii_cols:
            data = data.drop(i).withColumnRenamed("masked_"+i,i)
        return data
    def writing(self,active_data):
        active_data.withColumn('year', year(col('date'))).withColumn('day', dayofmonth(col('date'))).withColumn("monthname", date_format(to_date("date", "MM"), "MMM")).write.partitionBy("year","monthname","day").mode("overwrite").parquet(self.stagging)

df=spark.read.option("multiline","true").format("JSON").load("s3://rakeshv-landingzone-batch07/configuration/app-config.json")



data=['active','viewership']
for i in data:
    print(i)
    try:
        obj=spark_job(df.first()['ingest-Actives']['source']['data-location']+i+"/",df.first()['masked-active']['source']['data-location']+i+"/",df.first()['masked-active']['destination']['data-location']+i+"/")
        try:
            data=obj.read_data_rawzone()
            print('Read_data')
        except:
            continue

        data_masked=obj.masking_columns(data,df.first()['masked-'+i]['masking-cols'])
        data_casted =obj.casting_decimal(data_masked,df.first()['masked-'+i]['transformation-cols'])
        data_transformed = obj.transformation(data_casted,df.first()['masked-'+i]['comma-seperate'])
        obj.writing(data_transformed)
        print("Data Transformed",i)
        lookup_data=obj.lookup_dataset(data_transformed,df.first()['lookup-dataset']['pii-cols-'+i],df.first()['lookup-dataset']['data-location-'+i])
        obj.writing(lookup_data)
        print("Data lookup_data",i)
    except:
        print('No data',i)