import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;

import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import java.util.HashMap;
/**
 * Created by aparna on 12/05/15.
 */



public class NACRSPrediction {
    private static void testRegression(JavaRDD<LabeledPoint> trainingData,
                                       JavaRDD<LabeledPoint> testData) {
        // Train a RandomForest model.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
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
    }

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setAppName("NACRSPredictionAnalysis").setMaster("yarn-client");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        String datapath = "/user/aparna/input/Converted.data";

        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(ctx.sc(), datapath).toJavaRDD();

        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.75, 0.25});

        JavaRDD<LabeledPoint> trainingData = splits[0];

        JavaRDD<LabeledPoint> testData = splits[1];

        System.out.println("\nRunning example of regression using RandomForest\n");

        testRegression(trainingData, testData);
        ctx.stop();

    }
}
