////////////////////////////////pseudo code////////////////////////////////
/*
rdd_g (groupbykey)
// resetting
val conf=new SparkConf().setAppName("RDD_G").setMaster("local")
val sc=new SparkContext(conf)

// data import
val lines=sc.textFile("edges.csv")

// mapping
val pairs=lines.map(line=>(line.split(",")(1).toInt,1)).filter(_._1%100=0)

// grouping
val grouped=pairs.groupByKey()
val counts=grouped.mapValues(_.size)

// print
counts.saveAsTextFile("output/RDD_G")
 */
///////////////////////////////////////////////////////////////////////////


/////////////////////////////////real code/////////////////////////////////
package spark_g;

// import
import org.apache.spark.SparkConf; // application 구성 설정 마스터, 메모리 설정, 코어 수 등
import org.apache.spark.api.java.JavaPairRDD; // spark RDD 개발
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext; // spark 진입점으로 java에서 사용하기 위함, 데이터 처리 및 분산 기술을 가져옴
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;

// descriptions
/*
1. data loading: edges.csv
2. filtering the keys: (1) at least 1 follower (2) divisible by 100
3. mapping - spark groupbykey
4. aggregation - reudcing
 */

public class RDD_G {

    public static void main(String[] args){
        // spark 설정 초기화
        SparkConf conf=new SparkConf().setAppName("RDD_G").setMaster("local");
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

        // 팔로워 grouping
        JavaPairRDD<Integer,Iterable<Integer>> groupedFollowers=filteredFollowers.groupByKey();
        JavaPairRDD<Integer,Integer> followerCounts=groupedFollowers.mapValues(new Function<Iterable<Integer>,Integer>(){
            public Integer call(Iterable<Integer> values) throws Exception{
                int sum=0;
                for (Integer val:values){
                    sum+=val;
                }
                return sum;
            }
        });

        // Shuffle 전 aggregation 여부 확인
        System.out.println("Execution Plan:");
        System.out.println(groupedFollowers.toDebugString());


        // output
        followerCounts.foreach(result->System.out.println("(" + result._1 + ", " + result._2 + ")"));
        followerCounts.saveAsTextFile("output/RDD_G");

        sc.close();
    }
}
///////////////////////////////////////////////////////////////////////////