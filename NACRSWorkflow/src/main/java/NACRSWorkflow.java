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


public class NACRSWorkflow {

    public static class ParseLine implements PairFunction<String, String, String[]> {
        public Tuple2<String, String[]> call(String line) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(line));
            String[] elements = reader.readNext();
            String key= elements[0];
            return new Tuple2(key, elements);
        }
    }

    public static void main(String args[])
    {
        SparkConf sparkConf = new SparkConf().setAppName("NACRSAnalysis").setMaster("yarn-client");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD <String> csvFile = ctx.textFile("/user/aparna/input/FullParsedNACRS.csv");

        JavaPairRDD<String, String[]> keyedRDD = csvFile.mapToPair(new ParseLine());

        List<Tuple2<String, String[]>> output1 = keyedRDD.collect();
        for (Tuple2<?, ?> tuple1 : output1) {
            System.out.println(tuple1._1() + ": " );

            String[] t= (String[]) tuple1._2();

            for(int i=0; i<t.length; i++)
                System.out.print(t[i]+" ");

            System.out.println();

        }

    }
}
