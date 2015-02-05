package FTSparkDriver;

import FTSparkDriver.FTDriver;

/**
 * Created by aparna on 05/02/15.
 */
public class ConstructingMap implements Runnable {
    FTDriver ftDriver;
    public ConstructingMap(FTDriver ftDriver)
    {
        this.ftDriver=ftDriver;
    }
    public void run()
    {
        ftDriver.processRdds();
        return;
    }
}
