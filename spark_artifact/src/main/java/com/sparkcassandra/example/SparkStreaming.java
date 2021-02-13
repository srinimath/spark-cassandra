package com.sparkcassandra.example;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class SparkStreaming {
    private SparkSession spark;
    private SparkContext sc;
    private StreamingContext ssc;
    public SparkStreaming(){
        spark = SparkSession.builder()
                            .master("local[*]")
                            .appName("Spark Streaming")
                            .getOrCreate();
        
        sc = spark.sparkContext();
        sc.setLogLevel("ERROR");

        ssc = new StreamingContext(sc, Durations.seconds(1));

    }

public static void main(String[] args) {

    SparkStreaming kafkaStream = new SparkStreaming();
    System.out.println("Application ID: " + kafkaStream.sc.applicationId());
    
}
    
}
