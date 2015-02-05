package FTSparkDriver;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.util.LinkedList;
import java.util.Queue;
/**
 * Created by aparna on 22/01/15.
 */
public class MyTailerListener extends TailerListenerAdapter{

     FTDriver ftDriver;

    public MyTailerListener(FTDriver ftDriver)
    {
        this.ftDriver=ftDriver;

    }
    public MyTailerListener()
    {

    }
    public void handle(String Line)
    {

        if(Line.matches(".*Starting job:.*"))
        {
   //         System.out.println("No lines "+ftDriver.no_lines);
            //System.out.println("Starting job "+Line);
           // ftDriver.processRdds();
            Thread t=new Thread(new ConstructingMap(ftDriver));
            t.start();
        }
        else if(Line.matches(".*Registering RDD.*"))
        {
            Thread t=new Thread(new processRegisteringRDD(Line, ftDriver));
            t.start();
        }
        else if(Line.matches(".*Successfully stopped SparkContext.*"))
        {
        //th.interrupt();
        }
        else if(Line.matches(".*finished in.*"))
        {
            Thread t=new Thread(new processFinishedStage(Line));
            t.start();
        }

    }
}


