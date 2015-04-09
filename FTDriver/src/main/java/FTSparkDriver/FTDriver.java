package FTSparkDriver;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;

/* Lines class is used for constructing a tree */
class lines
{
    String line;
    String name;
    String operation;
    int l_no;
}
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

    private Map<String,JavaRDD> m1= new HashMap<String, JavaRDD>();
    private Map<String,JavaPairRDD> m2=new HashMap<String, JavaPairRDD>();
     List<String> Stages=new ArrayList<String> ();

    /*Constructor */
    public FTDriver(persistRDDs WorkFlow, String logFile, String sourceFile)
    {
        System.out.println("Initializing Fault Tolerant Driver");
      //  InitializeTailer(logFile);
        this.WorkFlow=WorkFlow;
        no_lines=0;
        processSourceFile(sourceFile, WorkFlow);
       // processRdds();
     //   System.out.println("FileName "+FileName);

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
       // System.out.println("Number of lines is"+no_lines);
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
        finally
        {
            try{
                is.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

        }
        //print();
    }
    /* used to initialize the tailer */
    private void InitializeTailer(String LogFile)
    {
        tr=new Thread(new TailerThread(LogFile, this));
        tr.start();
    }

   /* helper function - used to check if the content of the file is stored correctly*/
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
        Iterator<Map.Entry<Integer,rddData>> entriesRdd = rddDataNumber.entrySet().iterator();
        while(entriesRdd.hasNext())
        {
            Map.Entry<Integer,rddData> entryRdd = entriesRdd.next();
            System.out.println("Hashmap RDD entry "+entryRdd.getKey()+" "+entryRdd.getValue().getName());
        }
    /*  try {
            Iterator<Map.Entry<String, JavaRDD>> entries2 = m1.entrySet().iterator();
            while (entries2.hasNext()) {
                Map.Entry<String, JavaRDD> entry = entries2.next();
                System.out.println("Hashmap entry " + entry.getKey() + " " + entry.getValue().toString());
            }


            Iterator<Map.Entry<String, JavaPairRDD>> entries1 = m2.entrySet().iterator();
            while (entries1.hasNext()) {
                Map.Entry<String, JavaPairRDD> entry = entries1.next();
                System.out.println("Hashmap entry " + entry.getKey() + " " + entry.getValue().toString());
            }
        }catch(Exception e)
        {
            e.printStackTrace();
        }*/
    }

    void cache_call(String name)
    {

    }

    public void processRdds()
    {
        Class cls=WorkFlow.getClass();
        System.out.println("Class "+cls.toString());
        Field[] fields=cls.getDeclaredFields();
        for(int i=0;i<fields.length;i++)
        {
            System.out.println(fields[i].getType());
            fields[i].setAccessible(true);
            if(fields[i].getType()==JavaRDD.class) {
                try {
                    System.out.println("Inside");
                    JavaRDD temp = (JavaRDD) fields[i].get(WorkFlow);

                    m1.put(fields[i].toString(), temp);
                //     temp.toString();
                }
                catch(Exception E)
                {
                    System.out.println("Trying to access fields in class "+FileName);
                    E.printStackTrace();
                }
            }
            else if(fields[i].getType().toString().equals("class org.apache.spark.api.java.JavaPairRDD"))
            {
                try {
                    JavaPairRDD temp = (JavaPairRDD) fields[i].get(WorkFlow);
                    m2.put(fields[i].toString(), temp);
                }
                catch(Exception E)
                {
                    System.out.println("Trying to access fields in class "+FileName);
                    E.printStackTrace();
                }
            }
        }
 //   m1.get("words3").cache();

    }

    /* Constructing the tree part */
    //This function will be called from the main function for every string which is a job...?
    public void constructTree(String DebugString)
    {
        CTree ctree = new CTree(this);

       ctree.parseLines(DebugString);
        ctree.processLines();
        ctree.calculateCriticality();
        System.out.println("******** GetPreOrder **********");
        ArrayList<Node> preOrder;
        int i;
        preOrder=ctree.getPreOrderTraversal();

        for(i=0;i<preOrder.size();i++)
        {
            System.out.println(preOrder.get(i).getName() +" "+preOrder.get(i).getCriticality()+" "+preOrder.get(i).getCritic_percentage());
        }
        System.out.println("**********STAGES!!!!********");
        for(String name: Stages)
            System.out.println(name);

    }

}
class CTree
{
    FTDriver ftDriver;
    int no_lines;
    List<lines> Lines=new ArrayList();
    Node root=null;
    HashMap<Integer, List<lines>> dependencies = new HashMap<Integer, List<lines>>();

    CTree(FTDriver ftDriver)
    {
        this.ftDriver=ftDriver;
    }
    /* Populate HashMap for creating tree */

    void parseLines(String DebugString)
    {

        String temp_strings[]=DebugString.split("\n");
        Thread workers[]=new Thread[temp_strings.length+1];
        try {

            for (int j = 0; j < temp_strings.length; j++) {
                lines temp = new lines();
                temp.line = temp_strings[j];
             //   System.out.println("Parsing Line " + temp_strings[j]);
                workers[j] = new Thread(new processRegisteringRDD(temp_strings[j], ftDriver));
                workers[j].start();
                Lines.add(temp);

            }
            no_lines = temp_strings.length;
            System.out.println(no_lines);

            /* waiting for all the worker threads to join */
            for (int j = 0; j < temp_strings.length; j++) {
                workers[j].join();
                workers[j] = null;
                System.out.println("Worker " + j + "joined");
            }
         // ftDriver.printMap();


        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    /* Used to getParent of previous Node */
    Node getParent(Node root, String name)
    {
        Node found=null;
        if(root.getName().equals(name))
            return root;

        for( Node n: root.getChildren())
        {
            Node temp=getParent(n, name);
            if(temp!=null)
                found=temp;
        }
        return found;
    }

    /* check if the node is already present */
    boolean check(Node root, String name)
    {
        boolean found=false;

        if(root.getName().equals(name))
            return true;

        for(Node n: root.getChildren())
        {
            boolean temp= check(n, name);
            found |= temp;
        }
        return found;
    }


    void processLines()
    {
        int i;
        for(i=0;i<no_lines;i++)
        {
            /* counting Space */
            lines temp_line;
            temp_line=Lines.get(i);

                /* replacing all extra spaces to get the string */
            String t=temp_line.line;
            t=t.replaceAll("( )+"," ");
            t=t.trim();
            System.out.println(t);

            String temp[]=t.split(" ");

                /* Counting the number of characters in the line */
            int count_spaces=0;

            while(!Character.isLetter(temp_line.line.charAt(count_spaces)))
            {
                count_spaces++;
            }
            //System.out.println("count Spaces "+count_spaces);
            int length=temp.length;

            String t1[] = temp[length-1].split(":");
            int l=Integer.parseInt(t1[1]);

            rddData rdd = ftDriver.getRddDataNumber(l);
            String name = rdd.getName();
            System.out.println("Line_no "+l+"Name "+name);
            temp_line.operation=rdd.getOperation();
            temp_line.l_no=rdd.getLineNo();
            temp_line.name=name;

            //temp_line.l_no=l;

            //temp_line.name=name;

            Node n=new Node(l,name, count_spaces);
            for(int j=0; j< length ; j++)
            {
                System.out.println(j+" "+temp[j]);
            }

            // Finding stages
            if(temp[0].contains("("))
            {
                ftDriver.Stages.add(name);
            }


            /* setting the root node of the tree */
            if(root==null) {
                root = n;
                continue;
            }

              /* checking if the name is already presnt -> This is useful in the case where we have input 1 */
            if(check(root,name))
            {
                System.out.println("Found "+name+" Hence Skipping insertion");
                continue;
            }


            System.out.println("***************Name********************** "+temp_line.name);
            System.out.println("Operation "+temp_line.operation);
                /* Inserting into the tree */
             /* iF No Root , Make it root and continue to next line */

            // System.out.println(root.name);
            lines parent=null;
            int flag=1; // Used to check if parent name is the previous line

             /* if operation is Union, add it to the list*/
            if(temp_line.operation.equals("union"))
            {
                List<lines> ListLines = new ArrayList<lines>();
                ListLines.add(temp_line);
                dependencies.put(count_spaces,ListLines);
                System.out.println("Adding "+temp_line.name+" to the hashmap");
            }

            else if(dependencies.containsKey(count_spaces))
            {

                //get List of values
                System.out.println("Acessing Hashmap");
                List<lines> ListLines;
                ListLines=dependencies.get(count_spaces);

                //If this is first child
                if(ListLines.size()==1)
                {
                    System.out.println("Adding "+temp_line.name+" to the hashmap");
                    ListLines.add(temp_line);
                    dependencies.put(count_spaces, ListLines);
                }
                else
                {
                    flag=0;
                    parent=ListLines.get(0);
                    dependencies.remove(count_spaces);

                }

            }
                /* this is general case */
            if(flag==1)
                parent = Lines.get(i-1);

            Node parentNode=null;
            if(parent.name!=null)
                parentNode=getParent(root, parent.name);

            if(parentNode!=null)
            {
                System.out.println("parent "+parentNode.getName());
                parentNode.addChild(n);
                n.setParent(parentNode);
            }

        }


    }

    public ArrayList<Node> getPreOrderTraversal() {
        ArrayList<Node> preOrder = new ArrayList<Node>();
        buildPreOrder(root, preOrder);
        return preOrder;
    }

    private void buildPreOrder(Node node, ArrayList<Node> preOrder) {
        preOrder.add(node);
        for (Node child : node.getChildren()) {
            buildPreOrder(child, preOrder);
        }
    }
    void calculateCriticality()
    {
        calculateCriticalityNumber(root,0);
        int number=totalNodes(root);
        System.out.println("The total number of nodes is "+number);
        calculateCriticalityPercentage(root, number);
    }
    void calculateCriticalityPercentage(Node root, int number)
    {
        root.setCritic_percentage((double) root.getCriticality()/ (double) number*100);
        if(root.getChildren().size()==0)
        {
            return;
        }
        for(Node n: root.getChildren())
        {
            calculateCriticalityPercentage(n, number);

        }

        return;
    }
    /* void calculateCriticalityNumber(Node root)
     {
         if(root.getChildren().size()==0)
         {
             root.criticality=0;
             return;
         }
         int sum=0;
         for(Node n: root.getChildren())
         {
             calculateCriticalityNumber(n);
             sum+=n.criticality;
         }
         root.criticality=sum+root.getChildren().size();
     }*/
    void calculateCriticalityNumber(Node root, int critic)
    {

        root.setCriticality(critic);
        if(root.getChildren().size()==0)
        {
            return;
        }

        for(Node n: root.getChildren())
        {
            calculateCriticalityNumber(n, critic+1);
        }
        return;
    }
    int totalNodes(Node root)
    {
        if(root.getChildren().size()==0)
        {
            return 0;
        }
        int sum=0;
        for(Node n:root.getChildren())
        {
            sum+=totalNodes(n);
        }
        sum+=root.getChildren().size();
        return sum;
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
