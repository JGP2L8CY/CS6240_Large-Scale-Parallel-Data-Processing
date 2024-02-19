////////////////////////////////pseudo code////////////////////////////////
/*
rdd_dset ()
// setting
val conf=new SparkConf().setAppName("RDD_DEST").setMaster("local")
val sc=new SparkContext(conf)

// data import
val df=spark.read.csv("edges.csv").toDF("user","follower")

// data filtering & aggregate
val counts=df.filter($"follower"%100==0).groupBy("user").count()

// output
counts.saveAsTextFile("output/RDD_DSET")
 */
///////////////////////////////////////////////////////////////////////////


/////////////////////////////////real code/////////////////////////////////
package spark_dset;

// import
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import static org.apache.spark.sql.functions.*;

import java.util.Iterator;

public class RDD_DSET {

    public static void main(String[] args){
        // create sparksession
        SparkSession spark=SparkSession.builder().appName("RDD_DSET").master("local").getOrCreate();

        // data import
        Dataset<Row> lines=spark.read().option("inferSchema","true").csv("input/edges.csv").toDF("userId","followerId");

        // filtering to 100
        Dataset<Row> filtered=lines.filter(col("userId").mod(100).equalTo(0));

        // aggregate
        Dataset<Row> followerCounts=filtered.groupBy("userId").agg(count("followerId").alias("followercount"));

        System.out.println("Execution Plan for DSET:");
        followerCounts.explain();
        followerCounts.explain(true);

        // output
        followerCounts.show();
        followerCounts.write().csv("output/RDD_DSET");
        spark.stop();
       }
}
///////////////////////////////////////////////////////////////////////////