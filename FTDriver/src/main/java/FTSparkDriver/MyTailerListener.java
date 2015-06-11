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
         /*   String temp[]=Line.split(" ");
            String t[]=temp[temp.length-1].split(":");
            int l = Integer.parseInt(t[1]);
            String FileLine=ftDriver.Filelines[l-1];

            String name="";

            if(FileLine.matches(".*=.*"))
            {
                FileLine=FileLine.replaceAll(" ","");
                String temp1[]=FileLine.split("=");
                name=temp1[0];
            }
            else
            {
                FileLine=FileLine.replaceAll(" ","");
                String temp1[]=FileLine.split("[.]");
                name=temp1[0];
            }
            System.out.println("Job is "+name);
            ftDriver.putRddNameNumber(l, name);*/
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
         /*   if(flag!=1) {
                 ftDriver.printStagesInfo();
                  ftDriver.runAlgorithm();
            }

            flag=1;
            System.out.println("SHUT DOWN!!!!!!!!!!!!!!!!!");*/
        //th.interrupt();
            ftDriver.printMap();
        }
        else if(Line.matches(".*Job.*finished.*"))
        {
         /*   String temp[]=Line.split(" ");
            String t[]=temp[temp.length-4].split(":");

            if(t[0].equals("NACRSWorkflow.java")) {
                int l = Integer.parseInt(t[1]);
                String FileLine = ftDriver.Filelines[l - 1];

                String name = "";

                if (FileLine.matches(".*=.*")) {
                    FileLine = FileLine.replaceAll(" ", "");
                    String temp1[] = FileLine.split("=");
                    name = temp1[0];
                } else {
                    FileLine = FileLine.replaceAll(" ", "");
                    String temp1[] = FileLine.split("[.]");
                    name = temp1[0];
                }
                System.out.println("Job is " + name);

            }
            else
                System.out.println("job is outside"); */

        }
        else if(Line.matches(".*finished in.*"))
        {
          /*  Thread t=new Thread(new processFinishedStage(Line,ftDriver));
            t.start();*/

        }

    }
}


