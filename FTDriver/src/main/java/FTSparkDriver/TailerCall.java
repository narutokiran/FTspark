package FTSparkDriver;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import java.io.File;
/**
 * Created by aparna on 22/01/15.
 */
public class TailerCall {
    Tailer tailer;
    String LogFile;
    public TailerCall(String LogFile)
    {
     this.LogFile=LogFile;
    }
    void create()
    {
        System.out.println("*****New Tailer Being Created******");
        TailerListener listener=new MyTailerListener();
        tailer=new Tailer(new File(LogFile),listener,100);
        tailer.run();
    }
    void stop()
    {

        tailer.stop();
    }
}
