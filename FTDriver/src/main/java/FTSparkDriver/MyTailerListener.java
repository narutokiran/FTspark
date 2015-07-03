package FTSparkDriver;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.util.LinkedList;
import java.util.Queue;
/**
 * 15/06/11 15:56:42 INFO DAGScheduler: Job 20 finished: collect at NACRSWorkflow.java:370, took 1.511266 s
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

        }
        else if(Line.matches(".*Added rdd.*"))
        {
        //    System.out.println("ADDING RDDDD "+Line);
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

                //  ftDriver.runAlgorithm();
             //   ftDriver.printStagesInfo();
            }

            flag=1;
            System.out.println("SHUT DOWN!!!!!!!!!!!!!!!!!");
        //th.interrupt();
         //   ftDriver.printMap();
        }
        else if(Line.matches(".*Job.*finished.*"))
        {


        }
        else if(Line.matches(".*finished in.*"))
        {
          /*  Thread t=new Thread(new processFinishedStage(Line,ftDriver));
            t.start();*/

             Thread t = new Thread(new ProcessFinishedJob(ftDriver, Line));
            t.start();

        }

    }
}


