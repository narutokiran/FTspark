package FTSparkDriver;

/**
 * Created by aparna on 04/02/15.
 */
public class processFinishedStage implements Runnable{
    String Line;
    FTDriver ftDriver;
    double time_to_complete;
    public processFinishedStage(String line, FTDriver ftDriver)
    {
        this.ftDriver=ftDriver;
        this.Line=line;
    }
    public void run()
    {
        int line_no= processLine();
        String name=ftDriver.getRddNameNumber(line_no);
        if(name!=null) {
            rddData rdd = ftDriver.getStagesRDD(name);
            rdd.setTime_to_compute(time_to_complete);
        }
    }
    int processLine()
    {
        String[] temp=Line.split(" ");
        String t=temp[8];
    //    System.out.println(t);
        String temp1[]=t.split(":");
        temp1[1]=temp1[1].substring(0,temp1[1].length()-1);
        int line_no=Integer.parseInt(temp1[1]);

        time_to_complete=Double.parseDouble(temp[temp.length-2]);
        return line_no;
    }

}
