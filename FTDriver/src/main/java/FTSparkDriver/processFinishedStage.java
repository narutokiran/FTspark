package FTSparkDriver;

/**
 * Created by aparna on 04/02/15.
 */
public class processFinishedStage implements Runnable{
    String Line;
    FTDriver ftDriver;
    public processFinishedStage(String line, FTDriver ftDriver)
    {
        this.ftDriver=ftDriver;
        this.Line=line;
    }
    public void run()
    {
        int line_no=processLine();
        call_cache(line_no);
        return;
    }
    int processLine()
    {
        String[] temp=Line.split(" ");
        String t=temp[8];
    //    System.out.println(t);
        String temp1[]=t.split(":");
        temp1[1]=temp1[1].substring(0,temp1[1].length()-1);
        int line_no=Integer.parseInt(temp1[1]);
        return line_no;
    }
    void call_cache(int lineNo)
    {
        String name=ftDriver.getRddNameNumber(lineNo);
        System.out.println("Calling Cache on Rdd "+name+" in the line "+lineNo);
        ftDriver.cache_call(name);
    }
}
