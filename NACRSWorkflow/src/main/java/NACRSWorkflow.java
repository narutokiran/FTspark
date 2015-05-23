import org.apache.hadoop.mapreduce.Cluster;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;


import java.io.*;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;


import java.text.Normalizer;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;


import FTSparkDriver.FTDriver;
import FTSparkDriver.persistRDDs;

/* consider removing the first column in data */
class Workflow implements persistRDDs, Serializable {

    JavaRDD<String> csvFile;
    JavaPairRDD<String, String[]> NULLRDD, RemovedNULL, FilteredRDD, CleanedRDD, convertedRDD, Demographics, Patient_Details;
    JavaRDD<Vector> parsedData;
    JavaPairRDD<String,Vector> parsedDataWithKey;
    JavaPairRDD<String,Integer> clusterKey;
    JavaPairRDD<String, Tuple2<String[], Integer> > clusterJoinedRDD,cluster0, cluster1, cluster2;
    JavaPairRDD<String, String> FormattedCluster0, FormattedCluster1, FormattedCluster2;

    public void cache(String name)
    {

    }

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

    void Workflow_start()
    {
        SparkConf sparkConf = new SparkConf().setAppName("NACRSAnalysis").setMaster("yarn-client");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        FTDriver ftDriver = new FTDriver(this,"/home/aparna/spark-1.1.1/logs/SparkOut.log","NACRSWorkflow.java");
         csvFile = ctx.textFile("/user/aparna/input/ParsedFullNACRS.csv");

         NULLRDD = csvFile.mapToPair(new ParseLine());

       RemovedNULL = NULLRDD.filter( new Function<Tuple2<String, String[]>,Boolean>(){

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

         FilteredRDD = RemovedNULL.filter(new Function<Tuple2<String, String[]>, Boolean>(){

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
        RemovedNULL.collect();

         CleanedRDD = FilteredRDD.filter(new Function<Tuple2<String, String[]>, Boolean>(){
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
        FilteredRDD.collect();
        // broadcast?
        final Map<String, Integer> Mapping = new HashMap<String, Integer>();
        // Acumulator????
        //

        final Map<String, Integer> Counting = new HashMap<String, Integer>();

        Counting.put("count1", 1);
        convertedRDD=null;

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
        convertedRDD.collect();


         Demographics = convertedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>() {
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

         Patient_Details = convertedRDD.mapToPair(new PairFunction<Tuple2<String, String[]>, String, String[]>() {
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
        Patient_Details.collect();
         parsedData = Patient_Details.map(
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

        parsedDataWithKey = Patient_Details.mapToPair(new PairFunction<Tuple2<String, String[]>, String, Vector>(){
            public Tuple2<String, Vector> call(Tuple2<String, String[]> t2) {
                String[] sarray = (String[]) t2._2();
                double[] values = new double[sarray.length];
                for (int i = 0; i < sarray.length; i++)
                    values[i] = Double.parseDouble(sarray[i]);

                return new Tuple2<String, Vector>(t2._1(), Vectors.dense(values));
            }
        });
        parsedDataWithKey.collect();


        int numClusters = 3;
        int numIterations = 100;
        final KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);


        clusterKey = parsedDataWithKey.mapToPair(new PairFunction<Tuple2<String, Vector>, String, Integer>(){
            public Tuple2<String, Integer> call (Tuple2<String, Vector> t2)
            {
                int result = clusters.predict(t2._2());
                return new Tuple2<String, Integer>(t2._1(), result);
            }

        });




        clusterJoinedRDD = CleanedRDD.join(clusterKey);

        clusterJoinedRDD.collect();
        cluster0 = clusterJoinedRDD. filter(new Function<Tuple2<String, Tuple2<String[], Integer>>,Boolean  >(){
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

        cluster0.collect();
         FormattedCluster0 = cluster0.mapToPair(new PairFunction<Tuple2<String, Tuple2<String[], Integer>>,String, String >(){
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String[],Integer>> t2){
                Tuple2<String[],Integer> tempTup = t2._2();
                String[] temp = (String[]) tempTup._1();
                String second="";

                for(int i=0;i<temp.length-1; i++)
                {
                    second+=temp[i];
                    second+=",";
                }
                second+=temp[temp.length-1];
                return new Tuple2<String, String>(t2._1(), second);
            }

        });
        // convert array to STring + "," + string format!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
      //  System.out.println("Formatted Cluster "+FormattedCluster0.toDebugString());

        ftDriver.constructTree(FormattedCluster0.toDebugString())
        ;
        FormattedCluster0.saveAsTextFile("NACRS/output/cluster0");

         cluster1 = clusterJoinedRDD. filter(new Function<Tuple2<String, Tuple2<String[], Integer>>,Boolean  >(){
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

        cluster1.collect();

         FormattedCluster1 = cluster1.mapToPair(new PairFunction<Tuple2<String, Tuple2<String[], Integer>>,String, String >(){
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String[],Integer>> t2){
                Tuple2<String[],Integer> tempTup = t2._2();
                String[] temp = (String[]) tempTup._1();
                String second="";

                for(int i=0;i<temp.length-1; i++)
                {
                    second+=temp[i];
                    second+=",";
                }
                second+=temp[temp.length-1];
                return new Tuple2<String, String>(t2._1(), second);
            }

        });
        System.out.println("Formatted Cluster "+FormattedCluster1.toDebugString());
        FormattedCluster1.saveAsTextFile("NACRS/output/cluster1");

         cluster2 = clusterJoinedRDD. filter(new Function<Tuple2<String, Tuple2<String[], Integer>>,Boolean  >(){
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
        cluster2.collect();
         FormattedCluster2 = cluster2.mapToPair(new PairFunction<Tuple2<String, Tuple2<String[], Integer>>,String, String >(){
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String[],Integer>> t2){
                Tuple2<String[],Integer> tempTup = t2._2();
                String[] temp = (String[]) tempTup._1();
                String second="";

                for(int i=0;i<temp.length-1; i++)
                {
                    second+=temp[i];
                    second+=",";
                }
                second+=temp[temp.length-1];
                return new Tuple2<String, String>(t2._1(), second);
            }

        });


        System.out.println("Formatted Cluster "+FormattedCluster2.toDebugString());
        FormattedCluster2.saveAsTextFile("NACRS/output/cluster2");


        String datapath = "/user/aparna/input/Converted.data";

        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(ctx.sc(), datapath).toJavaRDD();

        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.75, 0.25});

        JavaRDD<LabeledPoint> trainingData = splits[0];

        JavaRDD<LabeledPoint> testData = splits[1];

        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "variance";
        Integer maxDepth = 4;
        Integer maxBins = 32;
        Integer seed = 12345;

        final RandomForestModel model = RandomForest.trainRegressor(trainingData,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

        // Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        System.out.println("TestData "+testData.toDebugString());
        Double testMSE =
                predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<Double, Double> pl) {
                        Double diff = pl._1() - pl._2();
                        return diff * diff;
                    }
                }).reduce(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double a, Double b) {
                        return a + b;
                    }
                }) / testData.count();
        System.out.println("Test Mean Squared Error: " + testMSE);
        System.out.println("Learned regression forest model:\n" + model.toDebugString());
        ctx.stop();

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
public class NACRSWorkflow {

    public static void main(String args[]) {
        Workflow workflow = new Workflow();
        workflow.Workflow_start();
    }
}