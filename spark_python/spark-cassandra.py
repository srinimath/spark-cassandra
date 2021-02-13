# running on Spark 3.0 and Cassandra 3.11.9
import findspark
findspark.init()

# add jars for spark-cassandra connector for this to work!
# pyspark_cassandra needs to be built first for mapping with spark catalog
# refer https://medium.com/@jentekllc8888/tutorial-integrate-spark-sql-and-cassandra-complete-with-scala-and-python-example-codes-8307fe9c2901

import pyspark_cassandra
import pyspark
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *

conf=SparkConf()
sc= SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.appName("Spark Cassandra Example").master("local[*]")\
                            .config("spark.sql.catalog.payments", "com.datastax.spark.connector.datasource.CassandraCatalog")\
                            .config("spark.cassandra.connection.host", "localhost")\
                            .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
                            .getOrCreate()

def runQuery(query):
    return spark.sql(query)

def readCassandra(table_name, key_space):
    df = spark.read.format("org.apache.spark.sql.cassandra")\
                                    .option("table", table_name).option("keyspace", key_space)\
                                    .load()
    return df

def writeCassandra(df, table_name, key_space):
    df.write.format("org.apache.spark.sql.cassandra")\
                    .option("table", table_name).option("keyspace", key_space)\
                    .mode("append").save()


if __name__ == "__main__":

    try:
        # display all keyspaces        
        query = "show namespaces from payments"
        df_all_key_spc = runQuery(query)
        print('--- keyspaces ---')
        df_all_key_spc.show()

    except Exception as e:
        print('Failed to create/display keyspace!')
        print(e)
    
    try:
        # query tables (assuming they already exists)       
        query = "select * from payments.spark_interview.spark_tbl_1"             
        df_tbl_1 = runQuery(query)
        print('--- spark_tbl_1 ---')
        df_tbl_1.show()

        query = "select * from payments.spark_interview.spark_tbl_2"             
        df_tbl_2 = runQuery(query)
        print('--- spark_tbl_2 ---')
        df_tbl_2.show()

    except Exception as e:
        print('Failed to query tables!')
        print(e)

    try:
        # join and send results to another table       
        df_tbl_join = df_tbl_1.join(df_tbl_2, df_tbl_1.tid == df_tbl_2.tid).select(df_tbl_1.tid,df_tbl_1.cm15,df_tbl_2.amount)
        writeCassandra(df_tbl_join,"spark_tbl_join","spark_interview")
        
        # read and display data
        print('--- spark_tbl_join ---')
        df_tbl_read_join = readCassandra("spark_tbl_join","spark_interview")
        df_tbl_read_join.show()

    except Exception as e:
        print('Failed in join and write!')
        print(e)

    spark.stop()