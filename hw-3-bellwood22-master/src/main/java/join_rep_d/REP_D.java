package join_rep_d;

// import
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;

public class REP_D {

    public static void main(String[] args){
        // setting
        SparkSession spark=SparkSession.builder().appName("REP_D")
                .config("spark.driver.memory", "8g")
                .config("spark.executor.memory", "8g")
                .config("spark.executor.memoryOverhead", "3g")
                .config("spark.driver.memoryOverhead", "3g")
                .getOrCreate();
        long startTime = System.currentTimeMillis();

        // import
        Dataset<Row> edges=spark.read().option("inferSchema","true").csv("s3://mr-median-hwijong/input/edges.csv").toDF("src","dst");

        // create (A,B) (B,A) dataset
        Dataset<Row> reversedEdges = edges.select(col("dst").alias("newSrc"), col("src").alias("newDst"));

        // set df02 to replicate df
        Dataset<Row> broadcastedReversedEdges = functions.broadcast(reversedEdges);

        // join01
            // join (A,B) (B,A)
            // joined01 = col [A,B,C] = ["src", "dst(b)", "a"]
        Dataset<Row> joinedEdges = edges.join(broadcastedReversedEdges,
                edges.col("dst").equalTo(broadcastedReversedEdges.col("newSrc")), "inner");

        // join02
        Dataset<Row> triangles = joinedEdges.join(edges,
                        joinedEdges.col("newDst").equalTo(edges.col("dst"))
                                .and(joinedEdges.col("src").equalTo(edges.col("src"))), "inner")
                .select(joinedEdges.col("src"), joinedEdges.col("dst"), joinedEdges.col("newDst"));

        // print
        long triangleCount = triangles.distinct().count();
        System.err.println("Total Triangles: " + triangleCount);

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.err.println("Execution Time: " + duration + " milliseconds");

        spark.stop();

    }
}
