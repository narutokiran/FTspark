package FTSparkDriver;
import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

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

    private Map<Integer,String> rddNameNumber = new HashMap<Integer,String> ();

    private Map<Integer, rddData> rddDataNumber = new HashMap<Integer, rddData>();
    /*Constructor */
    public FTDriver(persistRDDs WorkFlow, String logFile, String sourceFile)
    {
        System.out.println("Initializing Fault Tolerant Driver");
        InitializeTailer(logFile);
        this.WorkFlow=WorkFlow;
        no_lines=0;
        processSourceFile(sourceFile, WorkFlow);

    }

    public void putRddNameNumber(int line_no, String name)
    {
        rddNameNumber.put(Integer.valueOf(line_no),name);
    }

    public String getRddNameNumber(int line_no)
    {
        return (String) rddNameNumber.get(Integer.valueOf(line_no));
    }

    public void putRddDataNumber(int line_no, rddData rdds)
    {
        rddDataNumber.put(new Integer(line_no), rdds);
    }
    public rddData getRddDataNumber(int line_no)
    {
        return (rddData) rddDataNumber.get(new Integer(line_no));
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
     //   print();
    }
    /* used to initialize the tailer */
    private void InitializeTailer(String LogFile)
    {
        tr=new Thread(new TailerThread(LogFile, this));
        tr.start();
    }

    /* helper function - used to check if the content of the file is stored correctly
    void print()
    {
        int i;
        for(i=0;i<no_lines;i++)
            System.out.println(i+" "+Filelines[i]);
    } */

    /* Stopping the tailer thread */
    public void close()
    {
        System.out.println("****Calling Stop*****");
        printMap();
        tailer.stop();
    }

    public void printMap()
    {
        Iterator<Map.Entry<Integer, String>> entries =  rddNameNumber.entrySet().iterator();
        while(entries.hasNext())
        {
            Map.Entry<Integer, String> entry=entries.next();
            System.out.println("Hashmap entry "+entry.getKey()+" "+entry.getValue());
        }
    }

}

/* helper class to implement the tailer object */
class TailerThread extends FTDriver implements Runnable
{
    String LogFile;
    FTDriver ftDriver;
    TailerThread(String LogFile, FTDriver ftDriver)
    {
        this.LogFile=LogFile;
        this.ftDriver=ftDriver;
    }
    public void run()
    {
        System.out.println("*******Initializing the Tailer*******");
        tailer=new TailerCall(LogFile, ftDriver);
        tailer.create();
    }
}
