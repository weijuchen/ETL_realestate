
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .getOrCreate()
    # read.py 內部
    df = spark.read.csv("/usr/local/airflow/include/data.csv",   header="true") 
    # df = spark.read.csv("./include/data.csv", header="true") 

    # /usr/local/airflow/include/data.csv that path must exist in your Spark containers


    df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()