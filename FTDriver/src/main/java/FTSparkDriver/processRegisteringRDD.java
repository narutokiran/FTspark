package FTSparkDriver;

/**
 * Created by aparna on 03/02/15.
 */
class processRegisteringRDD implements Runnable
{
    String line;
    rddData rdd;
    int no;
    String name;
    FTDriver ftDriver;
    public processRegisteringRDD( String line, FTDriver ftDriver)
    {
     //   System.out.println("Creating Thread");
        this.line=line;
        this.ftDriver=ftDriver;
       // System.out.println("Name is .... "+ ftDriver.FileName);
    }
    public void run()
    {
       // System.out.println(line);
        rdd=new rddData(this);
        rdd.processLine(line);
        synchronized(rdd) {
            no = rdd.getLineNo();
            name = rdd.getName();
        }
        synchronized(ftDriver) {
            ftDriver.putRddNameNumber(no, name);
            ftDriver.putRddDataNumber(no, rdd);
        }

        return;
    }
}