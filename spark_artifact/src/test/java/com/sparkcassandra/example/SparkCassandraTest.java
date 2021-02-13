package com.sparkcassandra.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Unit test for simple App.
 */
class SparkCassandraTest {
    /**
     * Rigorous Test.
     */
    private static SparkSession spark;
    private static SparkContext sc;
        
    
    @BeforeAll
    public static void init(){
        spark = SparkSession.builder().appName("Spark Cassandra Example").master("local[*]")
                        .config("spark.sql.catalog.payments", "com.datastax.spark.connector.datasource.CassandraCatalog")
                        .config("spark.cassandra.connection.host", "localhost")
                        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
                        .getOrCreate();
        sc = spark.sparkContext();
        sc.setLogLevel("ERROR");
        System.out.println("executing before test..");
    }

    @AfterAll
    public static void end(){
        spark.stop();
        System.out.println("Executing after test..");
    }
    @Test
    void testSparkCassandra() {
        assertEquals(1, 1);
    }
}
