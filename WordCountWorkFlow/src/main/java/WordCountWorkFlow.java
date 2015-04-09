/**
 * Created by aparna on 26/11/14.
 */

import FTSparkDriver.FTDriver;
import FTSparkDriver.persistRDDs;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;



class WordCount implements persistRDDs, Serializable {
    private static final Pattern SPACE = Pattern.compile(" ");
    private JavaPairRDD<String, Integer> count7,ones1, counts1,ones2,counts2,ones3,counts3,ones4,counts4,unions1, count5, unions2, count6;
    private JavaRDD<String> words1,lines2,words2,lines3,words3,lines4,words4,lines1;
    private JavaPairRDD<String, Integer> unions3;
    private Map<String,JavaRDD<String>> m1= new HashMap<String, JavaRDD<String>>();
    private Map<String,JavaPairRDD<String,Integer>> m2=new HashMap<String, JavaPairRDD<String,Integer>>();

    @Override
    public void cache(String nameofRdd)
    {

        if(m1.containsKey(nameofRdd)) {
            System.out.println("Found 1 "+nameofRdd);
            m1.get(nameofRdd).cache();
            return;
        }
        if(m2.containsKey(nameofRdd)) {
            System.out.println("Found 2 "+nameofRdd);
            m2.get(nameofRdd).cache();
            return;
        }
        System.out.println("Did not find "+nameofRdd);
    }
    public void workflow_start()
    {


        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("yarn-client");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

          FTDriver ftDriver = new FTDriver(this,"/home/aparna/spark-1.1.1/logs/SparkOut.log","WordCountWorkFlow.java");

        /* Part of Job1 */

        lines1 = ctx.textFile("input/input1.txt", 1);
        m1.put("lines1",lines1);


        System.out.println("---------------------Starting Node 1-----------------------");

        words1 = lines1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });
        m1.put("words1", words1);

        ones1 = words1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        m2.put("ones1",ones1);

        counts1 = ones1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        m2.put("counts1",counts1);


        System.out.println("---------------------Ending Node 1-----------------------");

          /* Part of Job2 */
        System.out.println("---------------------Starting Node 2-----------------------");
        lines2 = ctx.textFile("input/input2.txt", 1);
        m1.put("lines2",lines2);

        words2 = lines2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });
        m1.put("words2",words2);

         ones2 = words2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        m2.put("ones2",ones2);

        counts2 = ones2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        m2.put("counts2",counts2);


        System.out.println("---------------------Ending Node 2-----------------------");

                /*Node 3*/

        System.out.println("---------------------Starting Node 3-----------------------");
         lines3 = ctx.textFile("input/input3.txt", 1);
        m1.put("lines3",lines3);

        words3 = lines3.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });
        m1.put("words3",words3);

         ones3 = words3.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        m2.put("ones3",ones3);

        counts3 = ones3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        m2.put("counts3",counts3);

        System.out.println("---------------------Ending Node 3-----------------------");
               /*Node 4 */

        System.out.println("---------------------Starting Node 4-----------------------");
       lines4 = ctx.textFile("input/input4.txt", 1);
        m1.put("lines4",lines4);

        words4 = lines4.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });
        m1.put("words4", words4);

        ones4 = words4.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        m2.put("ones4",ones4);

        counts4 = ones4.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        m2.put("counts4",counts4);


        System.out.println("---------------------Ending Node 4-----------------------");

        System.out.println("---------------------Starting Node 5----------------------");

      unions1 = ctx.union(counts1, counts2);
        m2.put("unions1",unions1);

        //  System.out.println("The union of first two RDDs");
      count5 = unions1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });
        m2.put("count5",count5);


        System.out.println("---------------------Ending Node 5-----------------------");

        System.out.println("---------------------Starting Node 6----------------------");

         unions2 = ctx.union(counts3, counts4);
        m2.put("unions2",unions2);

        //  System.out.println("The union of first two RDDs");
        count6 = unions2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });
        m2.put("count6", count6);


        System.out.println("---------------------Ending Node 6-----------------------");

        System.out.println("---------------------Starting Node 7----------------------");

      unions3 = ctx.union(count5, count6);
        m2.put("unions3",unions3);

        count7 = unions3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });
        m2.put("count7",count7);

        System.out.println("---------------------Ending Node 7-----------------------");

        StorageLevel st = new StorageLevel();
        System.out.println(count7.toDebugString());
        ftDriver.constructTree(count7.toDebugString());
        count7.saveAsTextFile("WordCount/output");

        ctx.stop();

    }
}
public class WordCountWorkFlow
{
    public static void main(String args[])
    {
        WordCount wc=new WordCount();
        wc.workflow_start();
    }
}

