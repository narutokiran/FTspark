/**
 * Created by aparna on 26/11/14.
 */

import FTSparkDriver.FTDriver;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import FTSparkDriver.*;
import org.apache.spark.scheduler.JobLogger;

public final class WordCountWorkFlow {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {


        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("yarn-client");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        FTDriver ftDriver = new FTDriver(ctx,"/home/aparna/Workflow.xml");
        JobLogger logger= new JobLogger("aparna","tmp");
        ctx.sc().addSparkListener(logger);
        JavaRDD<String> lines1 = ctx.textFile("input/input1.txt", 1);
       // System.out.println(lines1.toDebugString());

        /* Part of Job1 */

        System.out.println("---------------------Starting Node 1-----------------------");
        JavaRDD<String> words1 = lines1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones1 = words1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts1 = ones1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

       // List<Tuple2<String, Integer>> output1 = counts1.collect();
       /* for (Tuple2<?, ?> tuple1 : output1) {
            System.out.println(tuple1._1() + ": " + tuple1._2());
        }*/

        System.out.println("---------------------Ending Node 1-----------------------");

          /* Part of Job2 */
        System.out.println("---------------------Starting Node 2-----------------------");
        JavaRDD<String> lines2 = ctx.textFile("input/input2.txt", 1);
        JavaRDD<String> words2 = lines2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones2 = words2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts2 = ones2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

       // List<Tuple2<String, Integer>> output2 = counts2.collect();
        /*for (Tuple2<?, ?> tuple2 : output2) {
            System.out.println(tuple2._1() + ": " + tuple2._2());
        }*/

        System.out.println("---------------------Ending Node 2-----------------------");

                /*Node 3*/

        System.out.println("---------------------Starting Node 3-----------------------");
        JavaRDD<String> lines3 = ctx.textFile("input/input3.txt", 1);
        JavaRDD<String> words3 = lines3.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones3 = words3.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts3 = ones3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        /*List<Tuple2<String, Integer>> output3 = counts3.collect();
        for (Tuple2<?, ?> tuple3 : output3) {
            System.out.println(tuple3._1() + ": " + tuple3._2());
        }*/
        System.out.println("---------------------Ending Node 3-----------------------");
               /*Node 4 */

        System.out.println("---------------------Starting Node 4-----------------------");
        JavaRDD<String> lines4 = ctx.textFile("input/input4.txt", 1);

        JavaRDD<String> words4 = lines4.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones4 = words4.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts4 = ones4.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

       /* List<Tuple2<String, Integer>> output4 = counts4.collect();
        for (Tuple2<?, ?> tuple4 : output3) {
            System.out.println(tuple4._1() + ": " + tuple4._2());
        }*/

        System.out.println("---------------------Ending Node 4-----------------------");

        System.out.println("---------------------Starting Node 5----------------------");

        JavaPairRDD<String, Integer> unions1 = ctx.union(counts1, counts2);

        //  System.out.println("The union of first two RDDs");
        JavaPairRDD<String, Integer> count5 = unions1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });

      /*  List<Tuple2<String, Integer>> output5 = count5.collect();
        for (Tuple2<?, ?> tuple5 : output5) {
            System.out.println(tuple5._1()+": "+tuple5._2());
        }*/

        System.out.println("---------------------Ending Node 5-----------------------");

        System.out.println("---------------------Starting Node 6----------------------");

        JavaPairRDD<String, Integer> unions2 = ctx.union(counts3, counts4);

        //  System.out.println("The union of first two RDDs");
        JavaPairRDD<String, Integer> count6 = unions2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });

       /* List<Tuple2<String, Integer>> output6 = count6.collect();
        for (Tuple2<?, ?> tuple6 : output6) {
            System.out.println(tuple6._1()+": "+tuple6._2());
        }*/

        System.out.println("---------------------Ending Node 6-----------------------");

        System.out.println("---------------------Starting Node 7----------------------");

        JavaPairRDD<String, Integer> unions3 = ctx.union(count5, count6);

        //  System.out.println("The union of first two RDDs");
        JavaPairRDD<String, Integer> count7 = unions3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });

      /*  List<Tuple2<String, Integer>> output7 = count7.collect();
        for (Tuple2<?, ?> tuple7 : output7) {
            System.out.println(tuple7._1()+": "+tuple7._2());
        }*/

        System.out.println("---------------------Ending Node 7-----------------------");

        count7.saveAsTextFile("WordCount/output");
        System.out.println(count7.toDebugString());
        ctx.stop();

    }
}
