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

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;



public class WordCountWorkFlow implements persistRDDs {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static JavaPairRDD<String, Integer> count7,ones1, counts1,ones2,counts2,ones3,counts3,ones4,counts4,unions1, count5, unions2, count6;
    public static JavaRDD<String> words1,lines2,words2,lines3,words3,lines4,words4,lines1;
   public static  JavaPairRDD<String, Integer> unions3;
   static Map<String,JavaRDD<String>> m1= new HashMap<String, JavaRDD<String>>();

    @Override
    public void persist(String nameofRdd)
    {
         StorageLevel st=new StorageLevel();
        System.out.println(m1.get(nameofRdd));
        m1.get(nameofRdd).persist(st.DISK_ONLY());
    }
    public static void main(String[] args) throws Exception {


        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("yarn-client");
        System.out.println("---------*******------"+sparkConf.toDebugString());
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
     //   FTDriver ftDriver = new FTDriver(ctx,"/home/aparna/Workflow.xml");

     //   JobLogger logger= new JobLogger("aparna","tmp");
      //  ctx.sc().addSparkListener(logger);
      //  Driver dr=new Driver();

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

        ones1 = words1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        counts1 = ones1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });


        System.out.println("---------------------Ending Node 1-----------------------");

          /* Part of Job2 */
        System.out.println("---------------------Starting Node 2-----------------------");
        lines2 = ctx.textFile("input/input2.txt", 1);
        words2 = lines2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

         ones2 = words2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        counts2 = ones2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });


        System.out.println("---------------------Ending Node 2-----------------------");

                /*Node 3*/

        System.out.println("---------------------Starting Node 3-----------------------");
         lines3 = ctx.textFile("input/input3.txt", 1);
        words3 = lines3.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

         ones3 = words3.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        counts3 = ones3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        System.out.println("---------------------Ending Node 3-----------------------");
               /*Node 4 */

        System.out.println("---------------------Starting Node 4-----------------------");
       lines4 = ctx.textFile("input/input4.txt", 1);

        words4 = lines4.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        ones4 = words4.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        counts4 = ones4.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });



        System.out.println("---------------------Ending Node 4-----------------------");

        System.out.println("---------------------Starting Node 5----------------------");

      unions1 = ctx.union(counts1, counts2);

        //  System.out.println("The union of first two RDDs");
      count5 = unions1.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });


        System.out.println("---------------------Ending Node 5-----------------------");

        System.out.println("---------------------Starting Node 6----------------------");

         unions2 = ctx.union(counts3, counts4);

        //  System.out.println("The union of first two RDDs");
        count6 = unions2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });


        System.out.println("---------------------Ending Node 6-----------------------");

        System.out.println("---------------------Starting Node 7----------------------");

      unions3 = ctx.union(count5, count6);

        //  System.out.println("The union of first two RDDs");
        count7 = unions3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        });


        System.out.println("---------------------Ending Node 7-----------------------");
        FTDriver ftDriver =new FTDriver(new WordCountWorkFlow());
        count7.saveAsTextFile("WordCount/output");

        System.out.println(count7.toDebugString());

        ctx.stop();

    }
}
/*class Driver extends WordCountWorkFlow
{
    public void call()
    {    /*  StorageLevel st=new StorageLevel();
        lines1.persist(st.DISK_ONLY_2());
        System.out.println("Calling persist");
        System.out.println(lines1.toDebugString());
        System.out.println(unions3.toDebugString());
        System.out.println(count7.toDebugString());
    }
}*/
