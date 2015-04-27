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

/* consider removing the first column in data */

public class NACRSWorkflow {

    public static class ParseLine implements PairFunction<String, String, String[]> {
        public Tuple2<String, String[]> call(String line) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(line));
            String[] elements = reader.readNext();
            String key = elements[0];
            return new Tuple2(key, elements);
        }
    }
    public static boolean checkInteger(String integer)
    {
        int i;

        boolean seenDigit=false;

        for(i=0;i<integer.length();i++)
        {

            char c = integer.charAt(i);

            if(c>='0' && c<='9')
            {
                seenDigit=true;
                continue;
            }
            return false;
        }
        return seenDigit;
    }

    public static boolean checkDouble(String DoubleString)
    {
        int i;

        boolean seenDigit= false;
        boolean seenDot = false;
        for(i=0; i<DoubleString.length();i++)
        {
            char c = DoubleString.charAt(i);

            if(c >= '0' && c<='9')
            {
                seenDigit=true;
                continue;
            }
            else if(c=='.' && !seenDot)
            {
                seenDot=true;
                continue;
            }
            return false;
        }
        return seenDigit;
    }
    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setAppName("NACRSAnalysis").setMaster("yarn-client");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> csvFile = ctx.textFile("/user/aparna/input/ParsedFullNACRS.csv");

        JavaPairRDD<String, String[]> NULLRDD = csvFile.mapToPair(new ParseLine());

        JavaPairRDD<String, String[]> RemovedNULL = NULLRDD.filter( new Function<Tuple2<String, String[]>,Boolean>(){

            public Boolean call (Tuple2<String, String[]> t2)
            {
                String[] temp = (String[]) t2._2();
                for(int i=0; i< temp.length; i++)
                {
                    if(temp[i].equals("NULL"))
                        return false;
                }
                return true;
            }
        });

        /* Filtering the dataset to contain only records of length 27 */

        JavaPairRDD<String, String[]> FilteredRDD = RemovedNULL.filter(new Function<Tuple2<String, String[]>, Boolean>(){

            public Boolean call(Tuple2<String, String[]> t2)
            {
                String[] temp = (String[]) t2._2();
                if(temp.length==27)
                    return true;
                else return false;
            }

        });

        /* Cleaning the dataset
         */


        JavaPairRDD<String, String[]> CleanedRDD = FilteredRDD.filter(new Function<Tuple2<String, String[]>, Boolean>(){
           public Boolean call(Tuple2<String, String[]> t2)
           {
               String[] temp = (String[]) t2._2();
               for(int i=0; i < temp.length ;i++)
               {

                   if(i==4 || i==6 || i==7 || i==8 || i==9 || i==11 || i==12 || i==14 || i==21 || i==24)
                   {
                        boolean result = checkInteger(temp[i]);
                       if(!result) {
                         //  System.out.println(" The key is "+t2._1()+" and the value is "+temp[i]);
                           return false;
                       }
                   }
                   else if(i==20)
                   {
                       boolean result = checkDouble(temp[i]);
                       if(!result){
                           return false;
                       }
                   }

               }
               return true;
           }
        });


        List<Tuple2<String, String[]>> output1 = CleanedRDD.collect();
        for (Tuple2<?, ?> tuple1 : output1) {
            System.out.println(tuple1._1() + ": ");

            String[] t = (String[]) tuple1._2();

            System.out.println(t.length);
            for (int i = 0; i < t.length; i++)
                System.out.print(t[i] + " ");


            System.out.println();

        }


       /* JavaPairRDD<String, String[]> keyedRDD = csvFile.mapToPair(new ParseLine());

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


        JavaPairRDD<String, String[]> convertedRDD=null;

        for(int i=1 ; i< 28 ;i++)
        {
            Mapping.clear();
            Counting.clear();
            Counting.put("count1", 1);
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
            else if(i==2 || i==3 || i==5 || i==10 || i==13 || i==15 || i==16 || i==17 || i==18 || i==19 || i==22 || i==23 || i==25 || i==26)
            {
                final int index=i;
                convertedRDD = convertedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>(){
                    @Override
                    public Tuple2<String, String[]> call(Tuple2<String, String[]> t2) {
                        String[] temp = (String[]) t2._2();

                        int last_count = Counting.get("count1");
                        int number=0;
                        if (!Mapping.containsKey(temp[index])) {
                            Mapping.put(temp[index], last_count + 1);
                            Counting.put("count1", last_count + 1);
                            number = last_count + 1;
                        }
                        else
                            number = Mapping.get(temp[index]);

                        temp[index] = Integer.toString(number);
                        return new Tuple2<String, String[]>(t2._1(), temp);

                    }


                });
            }
            else
                continue;


        }
        System.out.println("PRINTING CONVERTED RDDDDDDDD!!!!!!!!!!!!!!!!!!!!!");
        System.out.println(convertedRDD.toDebugString());

        List<Tuple2<String, String[]>> output1 = convertedRDD.collect();
        for (Tuple2<?, ?> tuple1 : output1) {
            System.out.println(tuple1._1() + ": ");

            String[] t = (String[]) tuple1._2();

            for (int i = 0; i < t.length; i++)
                System.out.print(t[i] + " ");


            System.out.println();

        }*/
    }
}