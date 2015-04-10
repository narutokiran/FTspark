package FTSparkDriver;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.util.LinkedList;
import java.util.Queue;
/**
 * Created by aparna on 22/01/15.
 */
public class MyTailerListener extends TailerListenerAdapter{

     FTDriver ftDriver;
    int flag;

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
   //
         //   Thread t=new Thread(new ConstructingMap(ftDriver));
           // t.start();
        }
        else if(Line.matches(".*Added rdd.*"))
        {
           // System.out.println("ADDING RDDDD "+Line);
            Thread t=new Thread(new processAddedRDD(Line,ftDriver));
            t.start();
        }
        else if(Line.matches(".*Registering RDD.*"))
        {
         //   Thread t=new Thread(new processRegisteringRDD(Line, ftDriver));
           // t.start();
        }
        else if(Line.matches(".*Remoting shut down.*"))
        {
            if(flag!=1) {
                 ftDriver.printStagesInfo();
                //  ftDriver.runAlgorithm();
            }

            flag=1;
            System.out.println("SHUT DOWN!!!!!!!!!!!!!!!!!");
        //th.interrupt();
        }
        else if(Line.matches(".*Job Finished.*"))
        {

        }
        else if(Line.matches(".*finished in.*"))
        {
            Thread t=new Thread(new processFinishedStage(Line,ftDriver));
            t.start();

        }

    }
}


