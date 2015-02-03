package FTSparkDriver;

/**
 * Created by aparna on 28/01/15.
 */
public class rddData {
    private String name;
    private String line;
    private String operation;
    private int line_no;
    private int rdd_no;
    processRegisteringRDD rddthread;

    public rddData(processRegisteringRDD rddthread)
    {
        this.rddthread=rddthread;
    }

    void processLine(String line)
    {
        this.line=line;
        String temp[] = line.split(" ");
        rdd_no=Integer.parseInt(temp[6]);
        operation=temp[7].substring(1);
        String t[]=temp[9].split(":");
        String t1=t[1].substring(0, t[1].length()-1);
        line_no=Integer.parseInt(t1);
        FindName();
        print();
    }
    void FindName()
    {
        String FileLine=rddthread.ftDriver.Filelines[line_no-1];
        System.out.println(FileLine);
        if(FileLine.matches(".*=.*"))
        {
        FileLine=FileLine.replaceAll(" ","");
        String temp[]=FileLine.split("=");
         name=temp[0];
        }
        else
        {
            FileLine=FileLine.replaceAll(" ","");
            String temp[]=FileLine.split("[.]");
            name=temp[0];
        }
        System.out.println(FileLine);
    }
    int getLineNo()
    {
        return line_no;
    }
    String getName()
    {
        return name;
    }
    int getRdd_no() { return rdd_no; };

    void print()
    {
        System.out.println("name "+name);
        System.out.println("line "+line);
        System.out.println("operation "+operation);
        System.out.println("line no "+line_no);
        System.out.println("rdd no "+rdd_no);
        System.out.println();
    }
}
