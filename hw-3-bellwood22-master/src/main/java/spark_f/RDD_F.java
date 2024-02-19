////////////////////////////////pseudo code////////////////////////////////
/*
rdd_f ()
// setting
val conf=new SparkConf().setAppName("RDD_F").setMaster("local")
val sc=new SparkContext(conf)

// data import
val lines=sc.textFile("edges.csv")

// data filtering & folding
val pairs=lines.map(line=>(line.split(",")(1).toInt,1)).filter(_._1%100==0)
val counts=paris.foldByKey(0)(_+_)

// output
counts.saveAsTextFile("output/RDD_F")
 */
///////////////////////////////////////////////////////////////////////////


/////////////////////////////////real code/////////////////////////////////
package spark_f;

// import
import org.apache.spark.SparkConf; // application 구성 설정 마스터, 메모리 설정, 코어 수 등
import org.apache.spark.api.java.JavaPairRDD; // spark RDD 개발
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; // spark 진입점으로 java에서 사용하기 위함, 데이터 처리 및 분산 기술을 가져옴
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Iterator;

public class RDD_F {

    public static void main(String[] args){
        // spark 설정 초기화
        SparkConf conf=new SparkConf().setAppName("RDD_F").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);

        // data import
        JavaRDD<String> lines = sc.textFile("input/edges.csv");

        // mapping
        // 1은 임시 count
        JavaPairRDD<Integer,Integer> followers=lines.mapToPair(line -> {
            String[] parts=line.split(",");
            return new Tuple2<>(Integer.parseInt(parts[0]),1);
        });

        // 사용자 id 100 divisable 필터링
        JavaPairRDD<Integer,Integer> filteredFollowers=followers.filter(x->x._1()%100==0);

        // 팔로워 reduce
        JavaPairRDD<Integer,Integer> followerCounts=filteredFollowers.foldByKey(0,new Function2<Integer,Integer,Integer>(){
            public Integer call(Integer a,Integer b) throws Exception{
                return a+b;
            }
        });

        System.out.println("Execution Plan for FoldByKey:");
        System.out.println(followerCounts.toDebugString());

        // output
        followerCounts.foreach(result->System.out.println("(" + result._1 + ", " + result._2 + ")"));
        followerCounts.saveAsTextFile("output/RDD_F");

        sc.close();
    }
}
///////////////////////////////////////////////////////////////////////////