package join_rep_r;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class REP_R {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("REP_R")
                .set("spark.driver.memory", "8g")
                .set("spark.executor.memory", "8g")
                .set("spark.executor.memoryOverhead", "3g")
                .set("spark.driver.memoryOverhead", "3g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        long startTime = System.currentTimeMillis();

        // 데이터 로드
        JavaRDD<String> rawData = sc.textFile("s3://mr-median-hwijong/input/edges.csv");

        // (A,B) 및 (B,A) 형태의 엣지 생성
        JavaPairRDD<String, String> edges = rawData.flatMapToPair(s -> {
            String[] parts = s.split(",");
            List<Tuple2<String, String>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(parts[0], parts[1]));
            pairs.add(new Tuple2<>(parts[1], parts[0])); // 반대 방향 엣지 추가
            return pairs.iterator();
        });

        // 이웃 목록 생성
        Map<String, Iterable<String>> neighborMap = edges.groupByKey().collectAsMap();

        // broadcast 변수로 이웃 목록 전달
        final Broadcast<Map<String, Iterable<String>>> broadcastedNeighbors = sc.broadcast(neighborMap);

        // 가능한 삼각형 찾기
        JavaRDD<String> triangles = edges.flatMap(
                (FlatMapFunction<Tuple2<String, String>, String>) tuple -> {
                    List<String> results = new ArrayList<>();
                    String A = tuple._1();
                    String B = tuple._2();

                    // A의 모든 이웃 찾기
                    Iterable<String> neighborsOfA = broadcastedNeighbors.value().getOrDefault(A, Collections.emptyList());
                    // C가 B와도 연결되어 있는지 확인하기 위해 B의 이웃 목록 가져오기
                    Iterable<String> neighborsOfB = broadcastedNeighbors.value().getOrDefault(B, Collections.emptyList());
                    // B의 이웃 목록을 Set으로 변환하여 검색 시간 단축
                    Set<String> neighborsOfBSet = new HashSet<>();
                    neighborsOfB.forEach(neighborsOfBSet::add);

                    for (String C : neighborsOfA) {
                        // A와 연결된 C가 B와도 연결되어 있는지 확인
                        if (!C.equals(B) && neighborsOfBSet.contains(C)) {
                            // 삼각형 후보 생성 (A, B, C)
                            results.add(A + "," + B + "," + C);
                        }
                    }
                    return results.iterator();
                }
        );

        // 삼각형 개수 계산
        long triangleCount = triangles.distinct().count();
        System.err.println("Total Triangles: " + triangleCount);

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.err.println("Execution Time: " + duration + " milliseconds");

        sc.stop();
    }
}