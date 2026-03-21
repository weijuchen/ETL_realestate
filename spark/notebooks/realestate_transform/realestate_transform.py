# README:
# SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# - Sometimes the script can stay stuck after "Passing arguments..."
# - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# The easiest way to solve that is to restart your Airflow instance
# astro dev kill && astro dev start
# Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# Import the SparkSession module

# import sys
# from pyspark.sql import SparkSession
# import os

# def main():

#     spark=SparkSession.builder.appName("RealestateETL").master("spark://spark-master:7077").getOrCreate()


from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType,DoubleType,LongType

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import explode, arrays_zip, from_unixtime,split,input_file_name,col,when,lpad,concat 
from pyspark.sql.types import DateType

import os
import sys

if __name__ == '__main__':

    def app():

        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        endpoint = os.environ.get("ENDPOINT")
        postgres_user = os.environ.get("POSTGRES_USER")
        postgres_pw = os.environ.get("POSTGRES_PASSWORD")



        # Create a SparkSession
        spark = (
            SparkSession.builder.appName("FormatRealestate")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
            .config("fs.s3a.access.key", access_key)
            .config("fs.s3a.secret.key", secret_key)
            .config("fs.s3a.endpoint", endpoint)
            .config("fs.s3a.connection.ssl.enabled", "false")
            .config("fs.s3a.path.style.access", "true")
            .config("fs.s3a.attempts.maximum", "1")
            .config("fs.s3a.connection.establish.timeout", "5000")
            .config("fs.s3a.connection.timeout", "10000")
            .getOrCreate()
        )

        # Read a JSON file from an MinIO bucket using the access key, secret key,
        # and endpoint configured above

        realestate_schema = StructType([
        StructField("鄉鎮市區", StringType(), True),
        StructField("交易標的", StringType(), True),
        StructField("土地位置建物門牌", StringType(), True),
        StructField("土地移轉總面積平方公尺", DoubleType(), True),
        StructField("都市土地使用分區", StringType(), True),
        StructField("非都市土地使用分區", StringType(), True),
        StructField("非都市土地使用編定", StringType(), True),
        StructField("交易年月日", StringType(), True),
        StructField("交易筆棟數", StringType(), True),
        StructField("移轉層次", StringType(), True),
        StructField("總樓層數", StringType(), True),
        StructField("建物型態", StringType(), True),
        StructField("主要用途", StringType(), True),
        StructField("主要建材", StringType(), True),
        StructField("建築完成年月", StringType(), True),
        StructField("建物移轉總面積平方公尺", DoubleType(), True),
        StructField("建物現況格局-房", IntegerType(), True),
        StructField("建物現況格局-廳", IntegerType(), True),
        StructField("建物現況格局-衛", IntegerType(), True),
        StructField("建物現況格局-隔間", StringType(), True),
            ## 有/無
        StructField("有無管理組織", StringType(), True),
        ## 有/無
        StructField("總價元", LongType(), True),
        StructField("單價元平方公尺", LongType(), True),
        StructField("車位類別", StringType(), True),
        StructField("車位移轉總面積平方公尺", DoubleType(), True),
        StructField("車位總價元", LongType(), True),
        StructField("備註", StringType(), True),
        StructField("編號", StringType(), True),
        StructField("主建物面積", DoubleType(), True),
        StructField("附屬建物面積", DoubleType(), True),
        StructField("陽台面積", DoubleType(), True),
        StructField("電梯", StringType(), True),
        ## 有/無
        StructField("移轉編號", StringType(), True)
    ])
        # SPARK_APPLICATION_ARGS = "realestate-market/real_estate1144/a_lvr_land_a.csv"
        # input_path = [
        #     "s3a://realestate-market/real_estate1141/a_lvr_land_a.csv",
        #     "s3a://realestate-market/real_estate1142/a_lvr_land_a.csv",
        #     "s3a://realestate-market/real_estate1143/a_lvr_land_a.csv",
        # ]
        input_path = "s3a://realestate-market/real_estate*/{a,f,h}_lvr_land_a.csv"

        # only for taipei city
        # input_path = "s3a://realestate-market/real_estate*/a_lvr_land_a.csv"
        # input_path = f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}"

        # only for testing
        # input_path="s3a://realestate-market/real_estate1141/a_lvr_land_a.csv"

        # input_path = [
        #     "s3a://realestate-market/real_estate1144/a_lvr_land_a.csv",
        #     "s3a://realestate-market/real_estate1144/h_lvr_land_a.csv"
        # ]

        output_path="s3a://realestate-market/formatted/final-version"

        # print(f"here is the input_path  {input_path}")

        df = spark.read.option("header", "true").option("encoding", "UTF-8").schema(realestate_schema).csv(input_path)

        # drop the second row which column name is written in English
        df = df.filter(col("鄉鎮市區") != "The villages and towns urban district")

        file_source_year_season=split(input_file_name(),"real_estate")[1].substr(1,4)
        print(f"here is the file_source_year_season  {file_source_year_season}")
        df=df.withColumn("檔案來源",file_source_year_season)

        #  convert  square meters  to ping
        df = df.withColumn(
            "單價元坪",
            when(
                col("單價元平方公尺").isNotNull(),
                (col("單價元平方公尺").cast("int") * 0.3025).cast("int"),
            ),
        )
        # convert ROC year to AD year
        # + could not work   only concat could  work

        df = df.withColumn(
            "西元_交易年月日",
            when(
                col("交易年月日").isNotNull(),
                concat(
                    (col("交易年月日").substr(1, 3).cast("int") + 1911).cast("string")
                    ,col("交易年月日").substr(4, 4).cast("string")
                )
            )
        )

        df = df.withColumn(
            "西元_建築完成年月日",
            when(
                col("建築完成年月").isNotNull(),
                concat(
                    (col("建築完成年月").substr(1, 3).cast("int") + 1911).cast("string")
                    ,col("建築完成年月").substr(4, 4).cast("string")
                ),
            ),
        )

        url = "jdbc:postgresql://mypostgres:5432/mydb"
        properties = {
         
             "user": postgres_user ,
     
             "password":postgres_pw,
            "driver": "org.postgresql.Driver"
        }

        # merge all csv files into one file
        # updated the modified csv file postgresSQL

        df.coalesce(1).write.jdbc(url=url, table="real_estate_table", mode="overwrite", properties=properties)

        # upload to minio
        # df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

        print(f"updating is successful , output_path: {output_path} ")

    app()
    os.system('kill %d' % os.getpid())
