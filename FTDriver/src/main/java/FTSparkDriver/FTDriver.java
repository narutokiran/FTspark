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

    /* the variable to call Apache Tailer's object */
    static TailerCall tailer;

    /* The thread to call Tailer */
    Thread tr;

    /*The Workflow object */
    persistRDDs WorkFlow;

    /* Information for source code transformation */

    /* no of lines in the file */
    int no_lines;

    /* The array for storing various lines */
    String [] Filelines;

    /* The name of the file */
    String FileName;

    /*Constructor */
    public FTDriver(persistRDDs WorkFlow, String logFile, String sourceFile)
    {
        System.out.println("Initializing Fault Tolerant Driver");
        InitializeTailer(logFile);
        this.WorkFlow=WorkFlow;
        no_lines=0;
        processSourceFile(sourceFile, WorkFlow);

    }

    public FTDriver()
    {
        System.out.println("Initializing Fault Tolerant Driver");
    }


    /* Used to process the source File - find the number of lines and store them in a string */
    private void processSourceFile(String sourceFile, persistRDDs WorkFlow)
    {

        InputStream is = WorkFlow.getClass().getClassLoader().getResourceAsStream(sourceFile);

        Filelines=new String[no_lines+2];
        this.FileName=sourceFile;
        try{
            BufferedReader br=new BufferedReader(new InputStreamReader(is));
            String line="";
            while((line=br.readLine())!=null)
            {
                no_lines++;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        System.out.println("Number of lines is"+no_lines);
        Filelines=new String[no_lines+2];
        is = WorkFlow.getClass().getClassLoader().getResourceAsStream(sourceFile);
        try{
            BufferedReader br=new BufferedReader(new InputStreamReader(is));
            String line="";
            int count=0;
            while((line=br.readLine())!=null)
            {
               Filelines[count++]=line;
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        print();
    }


    /* used to initialize the tailer */
    private void InitializeTailer(String LogFile)
    {
        tr=new Thread(new TailerThread(LogFile));
        tr.start();
    }

    /* helper function - used to check if the content of the file is stored correctly */
    void print()
    {
        int i;
        for(i=0;i<no_lines;i++)
            System.out.println(i+" "+Filelines[i]);
    }

    /* Stopping the tailer thread */
    public void close()
    {
        System.out.println("****Calling Stop*****");
        tailer.stop();
    }

}

/* helper class to implement the tailer object */
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
