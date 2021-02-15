package com.sparkcassandra.example;
import org.apache.spark.api.java.*; 
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
* Spark job to connect and query cassandra deployed to local.
*/
public class SparkCassandra {
    SparkSession spark;
    SparkContext sc;
    public SparkCassandra() {
        spark = SparkSession.builder().appName("Spark Cassandra Example").master("local[*]")
                        .config("spark.sql.catalog.payments", "com.datastax.spark.connector.datasource.CassandraCatalog")
                        .config("spark.cassandra.connection.host", "localhost")
                        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
                        .getOrCreate();
        sc = spark.sparkContext();
        sc.setLogLevel("ERROR");        
        
    }

    public Dataset<Row> runQuery(String query) {
        return spark.sql(query);
    }

    public Dataset<Row> readCassandra(String keySpace, String tableName) {
        Dataset<Row> df = spark.read().format("org.apache.spark.sql.cassandra")
										.option("table", tableName).option("keyspace", keySpace)
										.load();
		return df;
    }

    public JavaRDD<Row> readCassandraRDD(String keySpace, String tableName) {
        JavaRDD<Row> rdd = spark.read().format("org.apache.spark.sql.cassandra")
										.option("table", tableName).option("keyspace", keySpace)
										.load().toJavaRDD();
		return rdd;
    }

    public void writeCassandra(Dataset<Row> df, String keySpace, String tableName){
        System.out.println("Writing to cassandra " + keySpace + " " + tableName);
        df.write().format("org.apache.spark.sql.cassandra")
						.option("table", tableName).option("keyspace", keySpace)
						.mode("overwrite").save();
        System.out.println("Writing to cassandra completed " + keySpace + " " + tableName);
    }
    
    public static void main(String[] args) {
        SparkCassandra payments = new SparkCassandra();
        System.out.println("Application ID - " + payments.sc.applicationId());

        payments.spark.sql("show databases").show();
        Dataset<Row> df_spark_tbl_1 = payments.runQuery("select * from payments.spark_interview.spark_tbl_1");
        System.out.println("--spark_tbl_1--");
        df_spark_tbl_1.show();
        
        Dataset<Row> df_spark_tbl_2 = payments.runQuery("select * from payments.spark_interview.spark_tbl_2");
        System.out.println("--spark_tbl_2--");
        df_spark_tbl_2.show();

        Dataset<Row> df_spark_tbl_join = df_spark_tbl_1.join(df_spark_tbl_2,df_spark_tbl_1.col("tid").equalTo(df_spark_tbl_2.col("tid"))).select(df_spark_tbl_1.col("tid"),df_spark_tbl_1.col("cm15"),df_spark_tbl_2.col("amount"));
        System.out.println("--spark_tbl_join--");
        df_spark_tbl_join.show();

        payments.writeCassandra(df_spark_tbl_join, "spark_interview", "spark_tbl_join");

        Dataset<Row> df_read_cassandra = payments.readCassandra("spark_interview", "spark_tbl_join");

        System.out.println("--read from Cassandra--");
        df_read_cassandra.show();

        payments.spark.stop();
        

    }
}
