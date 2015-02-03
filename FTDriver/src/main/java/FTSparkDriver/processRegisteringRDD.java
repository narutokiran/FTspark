package FTSparkDriver;

/**
 * Created by aparna on 03/02/15.
 */
class processRegisteringRDD extends MyTailerListener implements Runnable
{
    String line;
    rddData rdd;
    int no;
    String name;
    public processRegisteringRDD(FTDriver ftDriver, String line)
    {
        this.ftDriver=ftDriver;
        this.line=line;
    }
    public void run()
    {
        System.out.println(line);
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