package graph_rdd;

// import
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Graph_RDD{

    public static void main(String[] args) {
        // spark
        SparkConf conf = new SparkConf().setAppName("Graph_RDD")
                .set("spark.driver.memory", "8g")
                .set("spark.executor.memory", "8g")
                .set("spark.executor.memoryOverhead", "3g")
                .set("spark.driver.memoryOverhead", "3g");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // set up the k
        int k = 3;
        String filePath = "s3://mr-median-hwijong/output/Graph.csv";
        // String filePath = "/input/Graph.csv";
        createGraph(sc, k, filePath);
        sc.stop();
    }

    // main
    public static void createGraph(JavaSparkContext sc, int k, String filePath) {
        // Using flatMap to generate graph data efficiently
        JavaRDD<String> lines = sc.parallelize(range(0, k), k).flatMap(new FlatMapFunction<Integer, String>() {
            @Override
            public Iterator<String> call(Integer chain) {
                List<String> links = new ArrayList<>();
                int startPage = chain * k + 1;
                for (int i = 0; i < k; i++) {
                    int source = startPage + i;
                    int destination = (i == k - 1) ? 0 : source + 1;
                    links.add(source + "," + destination);
                }
                return links.iterator();
            }
        });

        // Save the generated links to S3
        lines.saveAsTextFile(filePath);
    }

    private static List<Integer> range(int start, int end) {
        List<Integer> range = new ArrayList<>(end - start + 1);
        for (int i = start; i < end; i++) {
            range.add(i);
        }
        return range;

    }
}
