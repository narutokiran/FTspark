package FTSparkDriver;

/**
 * 15/06/12 13:35:15 INFO DAGScheduler: Stage 56 (reduce at NACRSWorkflow.java:531) finished in 0.183 s
 * Created by aparna on 12/06/15.
 */
public class ProcessFinishedJob implements Runnable {

    String Line;

    FTDriver ftDriver;
    String name;
    double time;

    ProcessFinishedJob(FTDriver ftDriver, String Line)
    {
        this.ftDriver=ftDriver;
        this.Line=Line;
        name="";
    }

    public void run()
    {
        processLines();

        if(!ftDriver.containsKeyStagesRDD(name))
            return;

        rddData rdd=ftDriver.getStagesRDD(name);

        synchronized(rdd)
        {
            double temp= rdd.getTime_to_compute();
            time+=temp;
            rdd.setTime_to_compute(time);
        }
        return;

    }

    public void processLines()
    {
        String temp[] = Line.split(" ");
        String t[] = temp[temp.length - 5].split(":");

        if (t[0].equals("NACRSWorkflow.java")) {
            t[1] = t[1].substring(0, t[1].length()-1);
            int l = Integer.parseInt(t[1]);
            String FileLine = ftDriver.Filelines[l - 1];



            if (FileLine.matches(".*=.*")) {
                FileLine = FileLine.replaceAll(" ", "");
                String temp1[] = FileLine.split("=");
                name = temp1[0];
            } else {
                FileLine = FileLine.replaceAll(" ", "");
                String temp1[] = FileLine.split("[.]");
                name = temp1[0];
            }
           // System.out.println("Job is " + name);

        } else
            System.out.println("job is outside");
//
        time=Double.parseDouble(temp[temp.length-2]);
    }

}
