/**
 * Created by aparna on 16/04/15.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;


import java.io.*;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import java.util.Map;
import java.util.HashMap;



public class NACRSWorkflow {

    public static class ParseLine implements PairFunction<String, String, String[]> {
        public Tuple2<String, String[]> call(String line) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(line));
            String[] elements = reader.readNext();
            String key = elements[0];
            return new Tuple2(key, elements);
        }
    }

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setAppName("NACRSAnalysis").setMaster("yarn-client");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> csvFile = ctx.textFile("/user/aparna/input/FullParsedNACRS.csv");

        JavaPairRDD<String, String[]> keyedRDD = csvFile.mapToPair(new ParseLine());

        // broadcast?
        final Map<String, Integer> Mapping = new HashMap<String, Integer>();
        // Acumulator????
        //

        final Map<String, Integer> Counting = new HashMap<String, Integer>();

        Counting.put("count1", 1);

        // System.out.println(keyedRDD.toDebugString());

       /* for Printing out the RDD */
        /*
        List<Tuple2<String, String[]>> output1 = keyedRDD.collect();
        for (Tuple2<?, ?> tuple1 : output1) {
            System.out.println(tuple1._1() + ": " );

            String[] t= (String[]) tuple1._2();

            for(int i=0; i<t.length; i++)
                System.out.print(t[i]+" ");


            System.out.println();

        }
        */

        JavaPairRDD<String, String[]> convertedRDD=null;

        for(int i=1 ; i< 28 ;i++)
        {

            if(i==1)
            {
                convertedRDD = keyedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>(){
                    @Override
                    public Tuple2<String, String[]> call(Tuple2<String, String[]> t2) {
                        String[] temp = (String[]) t2._2();

                        int last_count = Counting.get("count1");
                        int number=0;
                        if (!Mapping.containsKey(temp[1])) {
                            Mapping.put(temp[1], last_count + 1);
                            Counting.put("count1", last_count + 1);
                            number = last_count + 1;
                        }
                        else
                            number = Mapping.get(temp[1]);

                        temp[1] = Integer.toString(number);
                        return new Tuple2<String, String[]>(t2._1(), temp);

                    }


                });
            }
            else if(i==2 || i==3 || i==5 || i==10 || i==13 || i==17 || i==18 || i==19 || i==22 || i==23 || i==25 || i==26 || i==27)
            {
                convertedRDD = keyedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>(){
                    @Override
                    public Tuple2<String, String[]> call(Tuple2<String, String[]> t2) {
                        String[] temp = (String[]) t2._2();

                        int last_count = Counting.get("count1");
                        int number=0;
                        if (!Mapping.containsKey(temp[1])) {
                            Mapping.put(temp[1], last_count + 1);
                            Counting.put("count1", last_count + 1);
                            number = last_count + 1;
                        }
                        else
                            number = Mapping.get(temp[1]);

                        temp[1] = Integer.toString(number);
                        return new Tuple2<String, String[]>(t2._1(), temp);

                    }


                });
            }
            else
                continue;


        }
        System.out.println(convertedRDD.toDebugString());
        List<Tuple2<String, String[]>> output1 = convertedRDD.collect();
        for (Tuple2<?, ?> tuple1 : output1) {
            System.out.println(tuple1._1() + ": ");

            String[] t = (String[]) tuple1._2();

            for (int i = 0; i < t.length; i++)
                System.out.print(t[i] + " ");


            System.out.println();

        }
    }
}
