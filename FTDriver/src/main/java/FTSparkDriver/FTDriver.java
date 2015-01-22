package FTSparkDriver;
import java.io.*;
import org.apache.spark.api.java.JavaSparkContext;
import japa.parser.JavaParser;
import japa.parser.ast.CompilationUnit;
import japa.parser.ast.visitor.VoidVisitorAdapter;
import japa.parser.ast.body.MethodDeclaration;
/**
 * Created by aparna on 04/12/14.
 */

public class FTDriver {
    static TailerCall tailer;
    Thread tr;
    persistRDDs WorkFlow;
    public FTDriver(persistRDDs WorkFlow, String logFile)
    {
        System.out.println("Initializing Fault Tolerant Driver");
        InitializeTailer(logFile);
        this.WorkFlow=WorkFlow;
    }
    public FTDriver()
    {
        System.out.println("Initializing Fault Tolerant Driver");
    }
    private void InitializeTailer(String LogFile)
    {
        tr=new Thread(new TailerThread(LogFile));
        tr.start();
    }
    public void close()
    {
        System.out.println("****Calling Stop*****");
        tailer.stop();
    }
}

class TailerThread extends FTDriver implements Runnable
{
    String LogFile;
    TailerThread(String LogFile)
    {
        this.LogFile=LogFile;
    }
    public void run()
    {
        System.out.println("*******Initializing the Tailer*******");
        tailer=new TailerCall(LogFile);
        tailer.create();
    }
}
