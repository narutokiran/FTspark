package FTSparkDriver;

/**
 * Created by aparna on 28/01/15.
 */
public class rddData {
    String name;
    String line;
    String operation;
    int line_no;
    int rdd_no;
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
        print();
    }

    void print()
    {
        System.out.println("line "+line);
        System.out.println("operation "+operation);
        System.out.println("line no "+line_no);
        System.out.println("rdd no "+rdd_no);
        System.out.println();
    }
}
