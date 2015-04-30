import org.apache.hadoop.mapreduce.Cluster;
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
import java.util.ArrayList;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

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

        // broadcast?
        final Map<String, Integer> Mapping = new HashMap<String, Integer>();
        // Acumulator????
        //

        final Map<String, Integer> Counting = new HashMap<String, Integer>();

        Counting.put("count1", 1);
        JavaPairRDD<String, String[]> convertedRDD=null;

        for(int i=1 ; i< 28 ;i++)
        {
            Mapping.clear();
            Counting.clear();
            Counting.put("count1", 1);
            if(i==1)
            {
                convertedRDD = CleanedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>(){
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


        JavaPairRDD<String, String[]> Demographics = convertedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>() {
            public Tuple2<String, String[]> call(Tuple2<String, String[]> t2) {
                String[] temp = (String[]) t2._2();
                List<String> al = new ArrayList<String>();
                for (int i = 0; i < temp.length; i++) {
                    if ( i == 2 || i == 3 || i == 12 || i == 13)
                        al.add(temp[i]);
                }
                int length = al.size();
                String array[] = new String[length];
                int i = 0;
                for (String s : al) {
                    array[i] = s;
                    i++;
                }
                return new Tuple2<String, String[]>(t2._1(), array);
            }

        });

        JavaPairRDD<String, String[]> Patient_Details = convertedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>() {
            public Tuple2<String, String[]> call(Tuple2<String, String[]> t2) {
                String[] temp = (String[]) t2._2();
                List<String> al = new ArrayList<String>();
                for (int i = 0; i < temp.length; i++) {
                    if (i==0 || i == 2 || i == 3 || i == 12 || i == 13 || i==23)
                       continue;
                    al.add(temp[i]);
                }
                int length = al.size();
                String array[] = new String[length];
                int i = 0;
                for (String s : al) {
                    array[i] = s;
                    i++;
                }
                return new Tuple2<String, String[]>(t2._1(), array);
            }

        }
        );

        JavaRDD<Vector> parsedData = Patient_Details.map(
                new Function<Tuple2<String, String[]>,Vector>() {
                    public Vector call(Tuple2<String, String[]> t2) {
                        String[] sarray = (String[]) t2._2();
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++)
                            values[i] = Double.parseDouble(sarray[i]);
                        return Vectors.dense(values);
                    }
                }
        );

        JavaPairRDD<String, Vector> parsedDataWithKey = Patient_Details.mapToPair(new PairFunction<Tuple2<String, String[]>, String, Vector>(){
                    public Tuple2<String, Vector> call(Tuple2<String, String[]> t2) {
                        String[] sarray = (String[]) t2._2();
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++)
                            values[i] = Double.parseDouble(sarray[i]);

                        return new Tuple2<String, Vector>(t2._1(), Vectors.dense(values));
                    }
        });


        int numClusters = 3;
        int numIterations = 100;
       final KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);


        JavaPairRDD<String, Integer> clusterKey = parsedDataWithKey.mapToPair(new PairFunction<Tuple2<String, Vector>, String, Integer>(){
            public Tuple2<String, Integer> call (Tuple2<String, Vector> t2)
            {
                int result = clusters.predict(t2._2());
                return new Tuple2<String, Integer>(t2._1(), result);
            }

        });

        List<Tuple2<String, Integer>> output2 = clusterKey.collect();
        for (Tuple2<?, ?> tuple1 : output2) {
            System.out.println(tuple1._1() + ": "+tuple1._2());




            System.out.println();

        }

        JavaPairRDD<String, Tuple2<String[], Integer> > clusterJoinedRDD = CleanedRDD.join(clusterKey);

        JavaPairRDD<String, Tuple2<String[], Integer> > cluster0 = clusterJoinedRDD. filter(new Function<Tuple2<String, Tuple2<String[], Integer>>,Boolean  >(){
           public Boolean call (Tuple2<String, Tuple2<String[], Integer>> t2)
           {
               Tuple2<String[], Integer> tempTup = t2._2();
               String[] temp =  (String[]) tempTup._1();
               int clusterNo = tempTup._2();

               if(clusterNo==0)
               {
                   return true;
               }
               return false;

           }
        });
        // convert array to STring + "," + string format!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        
        cluster0.saveAsTextFile("NACRS/output/cluster0");
        JavaPairRDD<String, Tuple2<String[], Integer> > cluster1 = clusterJoinedRDD. filter(new Function<Tuple2<String, Tuple2<String[], Integer>>,Boolean  >(){
            public Boolean call (Tuple2<String, Tuple2<String[], Integer>> t2)
            {
                Tuple2<String[], Integer> tempTup = t2._2();
                String[] temp =  (String[]) tempTup._1();
                int clusterNo = tempTup._2();

                if(clusterNo==1)
                {
                    return true;
                }
                return false;

            }
        });

        cluster0.saveAsTextFile("NACRS/output/cluster1");

        JavaPairRDD<String, Tuple2<String[], Integer> > cluster2 = clusterJoinedRDD. filter(new Function<Tuple2<String, Tuple2<String[], Integer>>,Boolean  >(){
            public Boolean call (Tuple2<String, Tuple2<String[], Integer>> t2)
            {
                Tuple2<String[], Integer> tempTup = t2._2();
                String[] temp =  (String[]) tempTup._1();
                int clusterNo = tempTup._2();

                if(clusterNo==2)
                {
                    return true;
                }
                return false;

            }
        });

        cluster0.saveAsTextFile("NACRS/output/cluster2");



       /* JavaPairRDD<String, String[]> keyedRDD = csvFile.mapToPair(new ParseLine());



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