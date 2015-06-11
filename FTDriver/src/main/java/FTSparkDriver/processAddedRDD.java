package FTSparkDriver;

/**
 * 15/06/11 15:34:39 INFO BlockManagerInfo: Added rdd_25_0 in memory on aparna-VirtualBox:38745 (size: 1616.0 B, free: 534.0 MB)
 * Created by aparna on 10/04/15.
 */
public class processAddedRDD implements Runnable {

    String line;
    FTDriver ftDriver;
    int rdd_no;
    double memory;
    String unit;
    processAddedRDD(String line, FTDriver ftDriver)
    {
     this.line=line;
        this.ftDriver=ftDriver;
    }
    public void run()
    {
        processLine();
        //Once line is processed, get the correpsonding rdd object from the map and then update the contents of the object


        if(!ftDriver.containsKeyRddDataRDDNumber(rdd_no))
            return;
        rddData rdd=ftDriver.getRddDataRDDNumber(rdd_no);



       synchronized (rdd)
       {
           double mem=rdd.getMemory_occupied();
           if(unit.equals("KB,"))
           {
               //System.out.println("Unit KBS");
               mem+=(memory/1000);
           }
           else if(unit.equals("B,"))
           {
               mem+=(memory/(1000*1000));
           }
           else
           mem+=(memory);
           rdd.setMemory_occupied(mem);
       }

    }


    void processLine()
    {
        String temp[] = line.split(" ");
        String rddNoString=temp[5];

        String t[]=rddNoString.split("_");
        rdd_no= Integer.parseInt(t[1]);

        memory=Double.parseDouble(temp[11]);

        unit=temp[12];
    }

    void print()
    {
        System.out.println("rdd "+rdd_no);
        System.out.println("memory "+memory);

    }
}
