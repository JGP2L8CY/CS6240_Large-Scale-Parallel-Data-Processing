/*
data import
    make (B,A) dataset

mapping
    (A,B)
    (B,A)

reducing
    data1.B, data2.B join
    reduce=


condition 2 = (C,A) or (A,C) search
*/
package join_rs_r;

// library
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RS_R {

    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("RS_R")
                .set("spark.driver.memory", "8g")
                .set("spark.executor.memory", "8g")
                .set("spark.executor.memoryOverhead", "3g")
                .set("spark.driver.memoryOverhead", "3g");
        JavaSparkContext sc=new JavaSparkContext(conf);
        long startTime = System.currentTimeMillis();


        // data import
        JavaRDD<String> rawData = sc.textFile("s3://mr-median-hwijong/input/edges.csv");
        // JavaRDD<String> rawData = sc.textFile("input/edges.csv");

        // create (A,B), (B,A)
        JavaPairRDD<String, String> pairs = rawData.flatMapToPair(s -> {
            String[] parts = s.split(",");
            return Arrays.asList(new Tuple2<>(parts[0], parts[1]), new Tuple2<>(parts[1], parts[0])).iterator();
        });

        // join, find (A,B,C)
        JavaPairRDD<String, Tuple2<String, String>> possibleTriangles = pairs.join(pairs);

        // FlatMap to find all (C, A) pairs that could form triangles
        JavaPairRDD<String, String> triangles = possibleTriangles.flatMapToPair(item -> {
            Tuple2<String, String> pair1 = item._2;
            return Arrays.asList(new Tuple2<>(pair1._1, pair1._2), new Tuple2<>(pair1._2, pair1._1)).iterator();
        }).distinct().filter(item -> pairs.lookup(item._1).contains(item._2));


        // Count unique triangles
        long triangleCount = triangles.count() / 3; // 삼각형은 세 번 계산되므로 3으로 나눕니다.
        System.err.println("Total triangles: " + triangleCount);


        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.err.println("Execution Time: " + duration + " milliseconds");

        sc.stop();
    }
}
