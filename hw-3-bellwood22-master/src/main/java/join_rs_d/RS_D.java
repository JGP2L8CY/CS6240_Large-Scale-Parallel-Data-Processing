/*
object: counts triangle-counts
    데이터셋 edges.csv를 자기 자신과 조인하여 (A,B)와 (B,C)의 관계 확보
    데이터셋 두개 확보 (A,B) 데이터셋, (B,A) 데이터셋
    1번 데이터셋의 value를 key 기준으로 2번 데이터셋의 key와 일치하는 조합 확보 (A,B,A) 형태가 됨
    조건 2는 조인 필요 없이 바로 (C,A) or (A,C)를 조인된 데이터셋에서 찾음

data import
    make (B,A) dataset

mapping

reducing
dataset 1에서 B와 dataset 2에서 B reduce-side join
    join dataset 1 and 2 -> (A,B,A)

condition 2 = (C,A) or (A,C) search
*/
package join_rs_d;

// import
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

public class RS_D {

    public static void main(String[] args){
        // spark setting
        SparkSession spark = SparkSession.builder().appName("RS_D")
                .config("spark.driver.memory", "8g")
                .config("spark.executor.memory", "8g")
                .config("spark.executor.memoryOverhead", "3g")
                .config("spark.driver.memoryOverhead", "3g")
                .getOrCreate();
        long startTime = System.currentTimeMillis();

        // dataset import
        Dataset<Row> edges = spark.read().option("inferSchema", "true").option("header", "false").csv("s3://mr-median-hwijong/input/edges.csv").toDF("src", "dst");
        Dataset<Row> reversedEdges = edges.select(col("dst").alias("src2"), col("src").alias("dst2"));

        // join_01
        Dataset<Row> joined = edges.as("edges1").join(reversedEdges.as("edges2"),
                col("edges1.dst").equalTo(col("edges2.src2")));

        // join_02
        Dataset<Row> triangles = joined.as("joined")
                .join(edges.as("edges3"), col("joined.dst2").equalTo(col("edges3.src")))
                .where(col("joined.src").equalTo(col("edges3.dst")));

        // output
        long trianglesCount = triangles.count();
        System.err.println("Total count: " + trianglesCount);

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.err.println("Execution Time: " + duration + " milliseconds");

        spark.stop();
    }
}