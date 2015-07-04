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
    int rdd_no;
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
    CTree ctree;


    private Map<Integer,String> rddNameNumber = new HashMap<Integer,String> ();

    private Map<Integer, rddData> rddDataNumber = new HashMap<Integer, rddData>();
  Map<Integer, rddData> rddDataRDDNumber = new HashMap<Integer, rddData>(); // This maps RDD object with RDD NUmber. Only for stages. FOr calcualting the amount of memory necessary

  Map<Integer , Node> TreeHash = new HashMap<Integer, Node>();
    private Map<String, rddData> StagesRDD = new HashMap<String, rddData>();

    ArrayList<Tree> roots = new ArrayList<Tree>();
    Map<String, Boolean> AlreadyCached = new HashMap<String, Boolean>() ;
    Map<String, Boolean> AlreadyPersisted = new HashMap<String, Boolean>() ;
    private Map<String,JavaRDD> m1= new HashMap<String, JavaRDD>();
    private Map<String,JavaPairRDD> m2=new HashMap<String, JavaPairRDD>();
    HashMap<String, Node> GroupDependencies = new HashMap<String, Node>();

     List<String> Stages=new ArrayList<String> ();
    List<String> RDDsToPersist=new ArrayList<String>();

    /*Constructor */
    public FTDriver(persistRDDs WorkFlow, String logFile, String sourceFile)
    {
        System.out.println("Initializing Fault Tolerant Driver");
       InitializeTailer(logFile);
        this.WorkFlow=WorkFlow;
        no_lines=0;
        processSourceFile(sourceFile, WorkFlow);
       // processRdds();
     //   System.out.println("FileName "+FileName);

    }


    public void putTreeHash(Integer number, Node TreeNode)
    {
        TreeHash.put(number,TreeNode);
    }

    public Node getTreeHash(String name)
    {
        return (Node) TreeHash.get(name);
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
    public boolean containsKeyRddDataRDDNumber(int no) { return rddDataRDDNumber.containsKey(no);
    }
    public boolean containsKeyStagesRDD(String name) { return StagesRDD.containsKey(name);}

    public rddData getRddDataRDDNumber(int rdd_no)
    {
        return (rddData) rddDataRDDNumber.get(rdd_no);
    }

    public void putrddDataRDDNumber(int rdd_no, rddData rdds ) {
        rddDataRDDNumber.put(rdd_no, rdds);
    }

    public void putStagesRDD(String name,  rddData rdds)
    {
        StagesRDD.put(name,rdds);
    }
    public rddData getStagesRDD(String name)
    {
        return (rddData) StagesRDD.get(name);
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
     /*   Iterator<Map.Entry<Integer, String>> entries =  rddNameNumber.entrySet().iterator();
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
        }*/
        Iterator<Map.Entry<Integer, rddData>> entries1 =  rddDataRDDNumber.entrySet().iterator();
        while(entries1.hasNext())
        {
            Map.Entry<Integer, rddData> entry=entries1.next();
            rddData rdd = (rddData) entry.getValue();
            System.out.println("Hashmap entry "+entry.getKey());
            rdd.print();
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

    public void printStagesInfo()
    {
        Iterator<Map.Entry<String, rddData>> entries =  StagesRDD.entrySet().iterator();
        double gain;
        double last_checkpoint_restore=0;
        double time_to_recompute_checkpoint=0;
        while(entries.hasNext())
        {
            Map.Entry<String, rddData> entry = entries.next();
            System.out.println("Hashmap entry " + entry.getKey());
            rddData rdd = (rddData) entry.getValue();
             rdd.print();
        }
    }

    void cache_call(String name)
    {

    }
    public void runAlgorithm()
    {

        for(int i=0;i<roots.size();i++)
        {
            for(int j=0;j<roots.get(i).roots.size();j++)
            {
                computeAlgorithm(roots.get(i).roots.get(j)); // The jth root in the ith tree
            }
        }
    }
    Times computeAlgorithm(Node root)
    {
        Times t=new Times();
        Times t1=new Times(); // for returning
        double time_to_recompute=0;
        double time_to_restore=0;
        double time_to_checkpoint=0;
        double gain=0;
        int flag_to_checkpoint=0;
        double critic_percent=0;
        for(Node n: root.getChildren())
        {
            t=computeAlgorithm(n);
            time_to_recompute += t.time_to_recompute;
            time_to_restore += t.time_to_restore;
        }
        if(root.getIsStage() && !root.checked)
        {
            String name=root.getName();

            System.out.println(name);

            rddData rdd= getStagesRDD(name);

            time_to_recompute+=rdd.getTime_to_compute();
             critic_percent=root.getCritic_percentage();

           time_to_checkpoint = rdd.getMemory_occupied() * 0.15625 *3;

            System.out.println("time to recompute "+time_to_recompute);
            System.out.println("time to checkpoint "+time_to_checkpoint);
            System.out.println("time to restore "+time_to_restore);

            if (time_to_checkpoint != 0) {
                gain = (time_to_restore + time_to_recompute - time_to_checkpoint) / time_to_checkpoint;
            }


            System.out.println("Gain is "+gain);
            gain = gain * 100;

            if (gain < -100) {

                System.out.println("Gain is less than -100%. DO NOT PERSIST");
                t1.time_to_recompute=time_to_recompute;
                t1.time_to_restore=time_to_restore;
            }
            else if (gain > 100) {
                System.out.println("Gain is more than 100%. CHECKPOINT!!!!");
                t1.time_to_recompute = 0;
                t1.time_to_restore= rdd.getMemory_occupied() * 0.1;
                RDDsToPersist.add(name);
                flag_to_checkpoint=1;
                System.out.println("******After checkpointing***********");
                System.out.println("Memory Occupied "+rdd.getMemory_occupied());
                System.out.println("time to recompute "+t1.time_to_recompute);
                System.out.println("time to checkpoint "+time_to_checkpoint);
                System.out.println("time to restore "+t1.time_to_restore);


            }
            else if(rdd.getTime_to_compute()!=0) {
                System.out.println("INSIDE ALGORITHM :) :) :) :) ");
               // t1.time_to_recompute = rdd.getTime_to_compute();
                //t1.time_to_restore= rdd.getMemory_occupied() *    0.1;

                //if -25% <=loss/gain <= 25% && criticality >75%:
                //persist

                if(gain >=-25 && gain <=25 && critic_percent>75 )
                {
                    System.out.println("Gain is between -25 and 25 and criticality is more than 75%!!!!");
                    t1.time_to_recompute = 0;
                    t1.time_to_restore= rdd.getMemory_occupied() * 0.1;
                    RDDsToPersist.add(name);
                    flag_to_checkpoint=1;
                    System.out.println("******After checkpointing***********");
                    System.out.println("Memory Occupied "+rdd.getMemory_occupied());
                    System.out.println("time to recompute "+t1.time_to_recompute);
                    System.out.println("time to checkpoint "+time_to_checkpoint);
                    System.out.println("time to restore "+t1.time_to_restore);
                }
                else if(gain >=25 && gain >=75 && critic_percent>50)
                {
                    System.out.println("Gain is between 25 and 75 and criticality is more than 50%!!!!");
                    t1.time_to_recompute = 0;
                    t1.time_to_restore= rdd.getMemory_occupied() * 0.1;
                    RDDsToPersist.add(name);
                    flag_to_checkpoint=1;
                    System.out.println("******After checkpointing***********");
                    System.out.println("Memory Occupied "+rdd.getMemory_occupied());
                    System.out.println("time to recompute "+t1.time_to_recompute);
                    System.out.println("time to checkpoint "+time_to_checkpoint);
                    System.out.println("time to restore "+t1.time_to_restore);
                }
                else if(gain >= 75 && gain<=100 && critic_percent>25)
                {
                    System.out.println("Gain is between 25 and 75 and criticality is more than 50%!!!!");
                    t1.time_to_recompute = 0;
                    t1.time_to_restore= rdd.getMemory_occupied() * 0.1;
                    RDDsToPersist.add(name);
                    flag_to_checkpoint=1;
                    System.out.println("******After checkpointing***********");
                    System.out.println("Memory Occupied "+rdd.getMemory_occupied());
                    System.out.println("time to recompute "+t1.time_to_recompute);
                    System.out.println("time to checkpoint "+time_to_checkpoint);
                    System.out.println("time to restore "+t1.time_to_restore);
                }
                else {
                    System.out.println("Conditions Not Satisfied");
                    t1.time_to_recompute=time_to_recompute;
                    t1.time_to_restore=time_to_restore;
                }

            }


        }
        else if(root.checked)
        {
            t1.time_to_recompute=root.time_to_recompute;
            t1.time_to_restore=root.time_to_restore;
        }
        else
        {
          t1=t;
        }
        if(!root.checked)
        {

            root.time_to_recompute=t1.time_to_recompute;
            root.time_to_restore=t1.time_to_restore;

            if(time_to_recompute!=0 || flag_to_checkpoint!=0)
            root.checked=true;
        }
        return t1;
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
       ctree = new CTree(this, TreeHash, roots);
        Stages.clear();
       ctree.parseLines(DebugString, FileName);
        ctree.processLines();

       ctree.calculateCriticality(roots);

    //    System.out.println("******** GetPreOrder **********");
        ArrayList<Node> preOrder;
        int i;
    //    System.out.println("Number fo trees is " + roots.size());
      /*  for( int j =0 ; j < roots.size(); j++) {
            preOrder = ctree.getPreOrderTraversal(roots.get(j));


            for (i = 0; i < preOrder.size(); i++) {
                System.out.println(preOrder.get(i).getName() + " " + preOrder.get(i).getCriticality() + " " + preOrder.get(i).getCritic_percentage());
            }
        }*/
     //   System.out.println("**********STAGES!!!!********");
   //     printMap();
        for(String name: Stages)
        {
            if(AlreadyCached.containsKey(name))
            {
                continue;
            }
  //          System.out.println("Processing Stage "+name);
            WorkFlow.cache(name);
            AlreadyCached.put(name, true);
        }

    }

    public void CheckForPersistance()
    {
     //   printStagesInfo();
        runAlgorithm();
        for(String name:RDDsToPersist)
        {
            if(AlreadyPersisted.containsKey(name))
            {
                continue;
            }
            System.out.println("Persisting RDD "+name);
            WorkFlow.persist(name);
        }
        RDDsToPersist.clear();
    }
}
class CTree
{
    FTDriver ftDriver;
    int no_lines;
    List<lines> Lines=new ArrayList();
    Node root;
    HashMap<Integer, List<lines>> dependencies = new HashMap<Integer, List<lines>>();
    HashMap<String, Node> GroupDependencies = new HashMap<String, Node>();
    Map<Integer, Node> TreeHash;
    ArrayList<Tree> roots;
    CTree(FTDriver ftDriver, Map<Integer, Node> TreeHash, ArrayList<Tree> roots)
    {
        this.ftDriver=ftDriver;
        root=null;
        this.TreeHash = TreeHash;
        this.roots=roots;
    }
    /* Populate HashMap for creating tree */

    void parseLines(String DebugString, String File)
    {

        String temp_strings[]=DebugString.split("\n");
        Thread workers[]=new Thread[temp_strings.length+1];
        int rdd_no=-1;
        int no_workers=0;
        no_lines=0;

        try {

            for (int j = 0; j < temp_strings.length; j++) {



                lines temp_l = new lines();
                temp_l.line = temp_strings[j];
             //   System.out.println("Parsing Line " + temp_strings[j]);
                String t = temp_strings[j];
                t = t.replaceAll("( )+", " ");
                t = t.trim();
                System.out.println("In ParseLines "+t);
                String temp[] = t.split(" ");
                String r="";
                String fName=temp[temp.length-2];
                String[] f = fName.split(":");
                String FileName = f[0];

                if(temp[1].contains("CachedPartitions") || temp[2].contains("CachedPartitions"))
                {
                    continue;
                }
                if(!FileName.equals(File))
                {
                    continue;
                }

                    if(temp[0].equals("|") && temp[1].equals("|"))
                {
                    if(temp.length==8) {
                        String rn[] = temp[2].split("\\[");
                        r = rn[1].substring(0, rn[1].length() - 1);
                    }
                    else
                    {
                        String rn[] = temp[3].split("\\[");
                        r = rn[1].substring(0, rn[1].length() - 1);
                        //System.out.println(r);
                    }
                    rdd_no=Integer.parseInt(r);
                }
                else
                {
                    if(temp.length==7) {
                        String rn[] = temp[1].split("\\[");
                        r = rn[1].substring(0, rn[1].length() - 1);
                        //  System.out.println(r);
                    }
                    else
                    {
                        String rn[] = temp[2].split("\\[");
                        r = rn[1].substring(0, rn[1].length() - 1);
                        // System.out.println(r);
                    }
                    rdd_no=Integer.parseInt(r);
                  //  System.out.println("rdd number is "+rdd_no);
                }
                if(!ftDriver.rddDataRDDNumber.containsKey(rdd_no)) {
                    workers[j] = new Thread(new processRegisteringRDD(temp_strings[j], ftDriver));
                    no_workers++;
                    workers[j].start();
                }
                temp_l.rdd_no=rdd_no;
                Lines.add(temp_l);
                no_lines++;

            }

          //  System.out.println(no_lines);

            /* waiting for all the worker threads to join */
            for (int j = 0; j <no_workers; j++) {
                workers[j].join();
                workers[j] = null;
         //       System.out.println("Worker " + j + "joined");
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
    Node getParent(Node root, int rdd_no)
    {
        Node found=null;
        if(root.getRdd_no()==rdd_no)
            return root;

        for( Node n: root.getChildren())
        {
            Node temp=getParent(n, rdd_no);
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

 /*   boolean checkParent(Node root, String name, String parent)
    {
        boolean found = false;

        if(root.getName().equals(name) && root.parent.getName().equals(parent))
            return true;

        for(Node n: root.getChildren())
        {
            boolean temp = checkParent(n, name, parent);
            found|= temp;
        }
        return found;
    }*/
    /* check if the node is already present */
    boolean check(Node root, String name , int rdd_no)
    {
        boolean found=false;

        if(root.getName().equals(name) && root.getRdd_no()==rdd_no)
            return true;

        for(Node n: root.getChildren())
        {
            boolean temp= check(n, name, rdd_no);
            found |= temp;
        }
        return found;
    }
    boolean check(Node root, int rdd_no)
    {
        boolean found=false;

        if( root.getRdd_no()==rdd_no)
            return true;

        for(Node n: root.getChildren())
        {
            boolean temp= check(n, rdd_no);
            found |= temp;
        }
        return found;
    }


    void processLines()
    {
        int i;
       boolean flag_root_changed=false;
        for(i=0;i<no_lines;i++)
        {
            boolean isStage=false;
            /* counting Space */
            lines temp_line;
            temp_line=Lines.get(i);

                /* replacing all extra spaces to get the string */
            String t=temp_line.line;
            t=t.replaceAll("( )+"," ");
            t=t.trim();
       //     System.out.println(t);

            String temp[]=t.split(" ");

                /* Counting the number of characters in the line */
            int count_spaces=0;

            while(!Character.isLetter(temp_line.line.charAt(count_spaces)))
            {
                count_spaces++;
            }
            //System.out.println("count Spaces "+count_spaces);
            int length=temp.length;

            int flagInside=0;

            String t1[] = temp[length-2].split(":");

            int l=Integer.parseInt(t1[1]);

            int rdd_no=-1;
            String r;
            if(temp[0].equals("|") && temp[1].equals("|"))
            {
                if(temp.length==8) {
                    String rn[] = temp[2].split("\\[");
                    r = rn[1].substring(0, rn[1].length() - 1);
                 //   System.out.println(r);
                }
                else
                {
                    String rn[] = temp[3].split("\\[");
                    r = rn[1].substring(0, rn[1].length() - 1);
                //    System.out.println(r);
                }
                rdd_no=Integer.parseInt(r);
            }
            else
            {
                if(temp.length==7) {
                    String rn[] = temp[1].split("\\[");
                    r = rn[1].substring(0, rn[1].length() - 1);
                //    System.out.println(r);
                }
                else
                {
                    String rn[] = temp[2].split("\\[");
                    r = rn[1].substring(0, rn[1].length() - 1);
                  //  System.out.println(r);
                }
                rdd_no=Integer.parseInt(r);
            }

            rddData rdd = ftDriver.getRddDataRDDNumber(rdd_no);

         //   System.out.println("THE CURRENT " +
         //           "rdd number is "+rdd_no);
            String name = rdd.getName();
          //  System.out.println("Line_no "+l+"Name "+name);
            temp_line.operation=rdd.getOperation();
      //      System.out.println("operation "+rdd.getOperation());
            //temp_line.l_no=rdd.getLineNo();
            temp_line.name=name;


            System.out.println("***************Name********************** "+temp_line.name);
            System.out.println("Operation "+temp_line.operation);

            //temp_line.l_no=l;

            Node currentNode=null;
            if(TreeHash.containsKey(rdd_no))
            {
                // check if it is present in same tree!!!
                boolean sameTree = true;
                if(root!=null)
                    sameTree = check(root, rdd_no);
            //    System.out.println("SameTREE???? "+sameTree);
                if(sameTree) {
                    currentNode=getParent(root, rdd_no);
                    flagInside = 1;
                }
                boolean differentTree = false;
                Tree tree=null;
                Node currentRoot=null;
                int index=-1;

                if(!sameTree || root==null)
                {
                    for(int j=0;j< roots.size(); j++)
                    {
                        tree = roots.get(j);

                        for(int k=0;k<tree.roots.size();k++)
                        {
                            currentRoot = tree.roots.get(k);

                            differentTree = check(currentRoot, name, rdd_no);

                            if(differentTree)
                            {
                                index = k;
                                break;
                            }
                        }
                        if(differentTree)
                        {
                            flagInside=1;
                            break;
                        }
                    }

                    if(differentTree && root == null) // This is the first node and is already present
                    {
                  //      System.out.println("Already presenet and first node... Exiting from tree!!! rdd no is "+rdd_no);
                        return;
                    }
                    if(differentTree && root!=null)
                    {

                        lines parent = Lines.get(i-1);
                 //       System.out.println("IN DIFFERENT TREE : Parent is "+parent.name);

                        Node parentNode = getParent(root, parent.rdd_no);
                        currentNode = getParent(currentRoot, name);

                        // Added to tree, now should update roots

                        //currentNode.addParent(parentNode);
                        parentNode.addChild(currentNode);

                    //    System.out.println("Current Node is "+currentNode.getName() + " Current root is "+currentRoot.getName());
                        if(currentNode!=currentRoot)
                        {


                            // for handling the special case

                                tree.roots.add(root);

                        }
                        else
                        {

                    //        System.out.println("In Else");
                            tree.roots.remove(index);

                            boolean alreadyPresent=false;

                            for(int k=0;k<tree.roots.size();k++)
                            {
                                if(tree.roots.get(k)==root)
                                    alreadyPresent=true;
                            }
                            if(!alreadyPresent)
                            tree.roots.add(root);
                        }
                        flag_root_changed=true;

                    }

                }
            }
         //   System.out.println("Flag Inside is "+flagInside);
            //temp_line.name=name;




            Node n=null;

            if(temp[0].contains("("))
            {
               // System.out.println("Adding "+name+" to Stages");
                if(currentNode==null || currentNode.isSetTime()==false) {
                    ftDriver.Stages.add(name);
                    isStage = true;

                    // Push the corresponding details into the map;

                    ftDriver.putStagesRDD(name, rdd);
                }
            }
            if(flagInside==0)
            n=new Node(l,name, count_spaces, isStage, rdd_no);
            else
            n=currentNode;

            if(isStage)
            {
                n.setIsStage(true);
            }

            // System.out.println(root.name);
            lines parent=null;
            int flag=1; // Used to check if parent name is the previous line

             /* if operation is Union, add it to the list*/
           //////////* if(temp_line.operation.equals("union"))
          /*dependencies.put(count_spaces,ListLines);
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

            }*/

            if(rdd.getOperation().equals("join")  ) // Only first time, not at other times
            {
                GroupDependencies.clear();
                GroupDependencies.put("Parent", n);
                //System.out.println("Inside Join");

            }
            else if(temp[0].contains("(") && GroupDependencies.size()==1)
            {

                GroupDependencies.put("Child1",n);

               // System.out.println("Inisde child1");
            }
            else if(temp[0].contains("(") && GroupDependencies.size()==2)
            {
                Node parentNode = GroupDependencies.get("Parent");

              //  System.out.println("Inisde child 2, Parent is "+parentNode.getName());
                if(flagInside==0) {
                    parentNode.addChild(n);
                    TreeHash.put(rdd_no, n);
                }
                GroupDependencies.clear();
                continue;
            }
                /* this is general case */
             /* setting the root node of the tree */
            if(root==null) {
                root = n;
                TreeHash.put(rdd_no, n);
                continue;
            }
            if(flag==1)
                parent = Lines.get(i-1);
           // System.out.println("Parent is (below flag ) "+ parent.name);

            if(check(root,name, rdd_no) )
            {


                Node n1=getParent(root, rdd_no);
              //  System.out.println("Node in found is "+n1.getRdd_no());

              //  System.out.println("Parent in check is "+n1.parent.getName() );
               /* if(n1.parent.getName()== parent.name)
                {
                    System.out.println("Found "+name+" Hence Skipping insertion");
                    continue;
                }
                else if(checkParent(root, name, parent.name))
                {
                    System.out.println("Found "+name+" Hence Skipping insertion case 2");
                    continue;
                }*/
                boolean found=false;
             //   System.out.println("Parent size is "+n1.getParents().size() );
                for(int k=0;k<n1.getParents().size();k++)
                {
             //       System.out.println("Parents inside found is "+n1.getParents().get(k).getName());
                    if(n1.getParents().get(k).getName().equals(parent.name)) {

                        found = true;
                    }
                }
                if(found)
                {
                //   System.out.println("Found "+name+" Hence Skipping insertion");
                    continue;
                }
                else
                {
                   // System.out.println("Parent RDD is "+parent.rdd_no);
                    Node parentNode = getParent(root, parent.rdd_no);
                    //System.out.println("Parent is " + parentNode.getName());
                   // parentNode.getChildren().add(n1);
                    parentNode.addChild(n1);

                    continue;

                }

            }
            Node parentNode=null;
            if(parent.name!=null)
                parentNode=getParent(root, parent.rdd_no);

            if(parentNode!=null && flagInside==0)
            {
     //           System.out.println("parent "+parentNode.getName());
                parentNode.addChild(n);
               // n.addParent(parentNode);
            }
            if(flagInside==0);
            TreeHash.put(rdd_no, n);

    //        System.out.println("Parnets are ");

        //    for(int k=0; k<n.getParents().size();k++)
         //       System.out.println(n.getParents().get(k).getName());
        }
        if(!flag_root_changed) {
            Tree newTree = new Tree();
            newTree.roots.add(root);
            roots.add(newTree);
        }
    }

    public ArrayList<Node> getPreOrderTraversal(Tree tree) {
        ArrayList<Node> preOrder = new ArrayList<Node>();

      //  System.out.println("The number of roots in the tree is "+ tree.roots.size());
        for(int i = 0 ;i< tree.roots.size();i++) {
     //       System.out.println("Root is "+tree.roots.get(i).getName());
            buildPreOrder(tree.roots.get(i), preOrder);
        }
        return preOrder;
    }

    private void buildPreOrder(Node node, ArrayList<Node> preOrder) {
        preOrder.add(node);
        for (Node child : node.getChildren()) {

            buildPreOrder(child, preOrder);
        }
    }
    void calculateCriticality(ArrayList<Tree> trees)
    {
        for(int i=0;i<trees.size();i++)
        {
            for(int j=0;j<trees.get(i).roots.size();j++)
            {
                clearCriticality(trees.get(i).roots.get(j)); // The jth root in the ith tree
            }
        }

        for(int i=0;i<trees.size();i++)
        {
            for(int j=0;j<trees.get(i).roots.size();j++)
            {
                calculateCriticalityNumber(trees.get(i).roots.get(j)); // The jth root in the ith tree
            }
        }
        for(int i=0;i<trees.size();i++)
        {
            for(int j=0;j<trees.get(i).roots.size();j++)
            {
                clearCount(trees.get(i).roots.get(j)); // The jth root in the ith tree
            }
        }
        int number = 0;
        for(int i=0;i<trees.size();i++)
        {
            for(int j=0;j<trees.get(i).roots.size();j++)
            {
                number+=totalNodes(trees.get(i).roots.get(j)); // The jth root in the ith tree
            }
        }
        System.out.println("Number of nodes is "+number);
        //totalNodes(root);
     //   System.out.println("The total number of nodes is "+number);
        for(int i=0;i<trees.size();i++)
        {
            for(int j=0;j<trees.get(i).roots.size();j++)
            {
                calculateCriticalityPercentage(trees.get(i).roots.get(j), number); // The jth root in the ith tree
            }
        }
    }

    void clearCriticality(Node root)
    {
        root.setCriticality(0);
        if(root.getChildren().size()==0)
        {
            return;
        }

        for(Node n: root.getChildren())
        {
            clearCriticality(n);
        }
        return;

    }

    void clearCount(Node root)
    {
        root.count=false;

        if(root.getChildren().size()==0)
            return;

        for(Node n:root.getChildren())
        {
            clearCount(n);
        }
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
    void calculateCriticalityNumber(Node root)
    {



        int t=0;



           if(root.getParents().size()==2)
           {
               t=Math.max(root.getParents().get(0).getCriticality(), root.getParents().get(1).getCriticality());
               //System.out.println("Multiple Paths "+root.getParents().get(0).getCriticality()+" "+root.getParents().get(1).getCriticality());

           }
        else {
               for (int i = 0; i < root.getParents().size(); i++) {
                   t += root.getParents().get(i).getCriticality();
               }
           }
        t+=root.getParents().size();
            root.setCriticality(t);

        if(root.getChildren().size()==0)
            return;

        for(Node n:root.getChildren())
        {
          //  System.out.println("Going into "+n.getName()+" "+n.getRdd_no()+" from "+root.getName()+" "+root.getRdd_no());
            calculateCriticalityNumber(n);
        }
    }
    int totalNodes(Node root)
    {
        if(root.getChildren().size()==0)
        {
            root.count=true;
            return 0;
        }
        if(root.count)
        {
            return 0;
        }
        int sum=0;
        for(Node n:root.getChildren())
        {
            sum+=totalNodes(n);
        }
        sum+=root.getChildren().size();
        root.count=true;
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

class Times
{
    double time_to_restore;
    double time_to_recompute;

    Times()
    {
        time_to_recompute=0;
        time_to_restore=0;
    }
}
