package transformation;

// import library
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TFM_07 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TFM_07");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // instances
        final double alpha = 0.15;
        final int iterations = 10;
        final int numberOfPages = 10000*10000;

        // data import
        // graph
        JavaRDD<String> lines = sc.textFile("s3a://mr-median-hwijong/input/Graph/");
        JavaPairRDD<Integer, Integer> graph = lines.mapToPair(s -> {
            String[] parts = s.split(",");
            return new Tuple2<>(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }).cache();

        // ranks
        JavaRDD<String> rankLines = sc.textFile("s3://mr-median-hwijong/input/Ranks.csv");
        JavaPairRDD<Integer, Double> ranks = rankLines.mapToPair(s -> {
            String[] parts = s.split(",");
            return new Tuple2<>(Integer.parseInt(parts[0]), Double.parseDouble(parts[1]));
        }).cache();

        // main
        JavaPairRDD<Integer, Tuple2<Integer, Double>> joined = graph.join(ranks).cache();
        for (int i=0; i<iterations;i++) {
            JavaPairRDD<Integer, Double> contributions = joined.flatMapToPair(item -> {
                        Integer pageId = item._1; // graphs key(node_id)
                        Integer link = item._2._1; // graph value
                        Double rank = item._2._2; // ranks value(pr_value)

                        List<Tuple2<Integer, Double>> results = new ArrayList<>();

                if (link == 0) {
                    results.add(new Tuple2<>(-1, rank));
                } else {
                    results.add(new Tuple2<>(pageId, rank));
                }

                return results.iterator();
            });

            // dangling pr
            JavaPairRDD<Integer, Double> danglingNodes = contributions.filter(item -> item._1 == -1);
            Double totalDanglingRankValue = danglingNodes.map(item -> item._2).reduce((a, b) -> a + b);
            final Broadcast<Double> totalDanglingRank = sc.broadcast(totalDanglingRankValue);

            Double pristinePR = 1 / (double) numberOfPages;
            double rankContributionFromDangling = pristinePR * totalDanglingRank.value();

            // get ranks
            JavaPairRDD<Integer, Double> updatedRanks = contributions
                    .filter(item -> item._1 != -1)
                    .reduceByKey((a, b) -> a + b)
                    .mapValues(rank -> (1 - alpha) / numberOfPages + alpha * (rank + rankContributionFromDangling));

            updatedRanks = updatedRanks.cache();

            if (i==iterations-1) {
                List<Tuple2<Integer, Double>> output = updatedRanks.take(10000);
                // JavaRDD<Tuple2<Integer, Double>> outputRDD = sc.parallelize(output);
                // outputRDD.saveAsTextFile("s3a://hw4-bellwood22-main_spark/output/RanksUpdated");
                System.out.println("Iteration " + (i + 1) + ":");

                for (Tuple2<?, ?> tuple : output) {
                    System.out.println("Node " + tuple._1 + " has rank: " + tuple._2);
                }
            }
        }
        sc.close();

    }
}