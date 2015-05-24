import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by aparna on 16/05/15.
 */

class CTreeNACRS
{
    HashMap<Integer, String> hm = new HashMap<Integer, String>();
    int no_lines;
    List<lines> Lines=new ArrayList();
    Node root;
    HashMap<Integer, List<lines>> dependencies = new HashMap<Integer, List<lines>>();
    HashMap<String, Node> GroupDependencies = new HashMap<String, Node>();
    HashMap<Integer, Node> TreeHash = new HashMap<Integer, Node>();
    ArrayList<Tree> roots= new ArrayList<Tree>();

    /* Populate HashMap for creating tree */
    void populateHashMap()
    {
      hm.put(352, "FormattedCluster0");
      hm.put(335, "Cluster0");
      hm.put(332, "ClusterJoinRDD");
      hm.put(145, "CleanedRDD");
      hm.put(129, "FilteredRDD");
      hm.put(113,"RemovedNULL");
      hm.put(111,"NULLRDD");
      hm.put(109, "csvFile");
      hm.put(320, "ClusterKey");
      hm.put(302, "ParsedDataWithKey");
      hm.put(268, "PatientDetails");
      hm.put(216, "ConvertedRDD");
      hm.put(190, "ConvertedRDD");
      hm.put(360, "Dummy");
    }


    void parseLines(String FileName)
    {
        int i=0;
        try
        {
            FileReader fr=new FileReader(FileName);
            BufferedReader br=new BufferedReader(fr);
            String line;
            while((line=br.readLine())!=null)
            {
                lines temp=new lines();
                temp.line=line;
                Lines.add(temp);
                i++;
            }
            no_lines=i;
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
        if(root.name.equals(name))
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
    boolean check(Node root, String name , int rdd_no)
    {
        boolean found=false;

        if(root.name.equals(name) && root.getRdd_no()==rdd_no)
            return true;

        for(Node n: root.getChildren())
        {
            boolean temp= check(n, name, rdd_no);
            found |= temp;
        }
        return found;
    }

    boolean checkParent(Node root, String name, String parent)
    {
        boolean found = false;

        if(root.name.equals(name) && root.parent.getName().equals(parent))
            return true;

        for(Node n: root.getChildren())
        {
            boolean temp = checkParent(n, name, parent);
                    found|= temp;
        }
        return found;
    }


    void processLines()
    {
        int i;
        boolean isNewTree = true;
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
            int rdd_no=-1;
            String r;
            if(temp[0].equals("|") && temp[1].equals("|"))
            {
                if(temp.length==8) {
                    String rn[] = temp[2].split("\\[");
                    r = rn[1].substring(0, rn[1].length() - 1);
                    System.out.println(r);
                }
                else
                {
                    String rn[] = temp[3].split("\\[");
                     r = rn[1].substring(0, rn[1].length() - 1);
                    System.out.println(r);
                }
                rdd_no=Integer.parseInt(r);
            }
            else
            {
                if(temp.length==7) {
                    String rn[] = temp[1].split("\\[");
                     r = rn[1].substring(0, rn[1].length() - 1);
                    System.out.println(r);
                }
                else
                {
                    String rn[] = temp[2].split("\\[");
                   r = rn[1].substring(0, rn[1].length() - 1);
                    System.out.println(r);
                }
                rdd_no=Integer.parseInt(r);
            }
            String t1[] = temp[length-2].split(":");
            int l=Integer.parseInt(t1[1]);
            String name = hm.get(l);

            temp_line.operation=temp[length-4];

            temp_line.l_no=l;

            temp_line.name=name;

            // check if TreeHash already contains the rdd

            if(TreeHash.containsKey(rdd_no))
            {
               // check if it is present in same tree!!!
                boolean sameTree = true;
                if(root!=null)
                 sameTree = check(root, name, rdd_no);
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
                            break;
                        }
                    }

                    if(differentTree && root == null) // This is the first node and is already present
                    {
                        System.out.println("Already presenet and first node... Exiting from tree!!! rdd no is "+rdd_no);
                        return;
                    }
                    if(differentTree && root!=null)
                    {
                       lines parent = Lines.get(i-1);
                        Node parentNode = getParent(root, parent.name);
                        Node currentNode = getParent(currentRoot, name);

                        // Added to tree, now should update roots

                        currentNode.parent = parentNode;
                        parentNode.addChild(currentNode);

                        if(currentNode!=currentRoot)
                        {
                            tree.roots.add(root);
                        }
                        else
                        {
                            tree.roots.remove(index);
                            tree.roots.add(root);
                        }

                    }
                    return;
                }
            }

            Node n=new Node(l,name, count_spaces, rdd_no);


            for(int j=0; j< length ; j++)
            {
                System.out.println(j+" "+temp[j]);
            }

            // Finding stages
            if(temp[0].contains("("))
            {
                System.out.println("FOUND STAGE!!!!!!");
            }


            /* setting the root node of the tree */
            if(root==null) {
                TreeHash.put(rdd_no,n);
                root = n;
                continue;
            }

              /* checking if the name is already presnt -> This is useful in the case where we have input 1 */



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

            if(temp_line.operation.equals("join"))
            {
                GroupDependencies.clear();
                GroupDependencies.put("Parent", n);

            }
            if(temp[0].contains("(") && GroupDependencies.size()==1)
            {
                GroupDependencies.put("Child1",n);
            }
            else if(temp[0].contains("(") && GroupDependencies.size()==2)
            {
                Node parentNode = GroupDependencies.get("Parent");

                System.out.println("Parent is "+parentNode.getName());
                parentNode.getChildren().add(n);
                GroupDependencies.clear();
                continue;
            }
                /* this is general case */
            if(flag==1)
                parent = Lines.get(i-1);
            if(check(root,name, rdd_no))
            {


                Node n1=getParent(root, name);
                System.out.println("Parent in check is "+n1.parent.getName() );
                if(n1.parent.getName()== parent.name)
                {
                    System.out.println("Found "+name+" Hence Skipping insertion");
                    continue;
                }
                else if(checkParent(root, name, parent.name))
                {
                    System.out.println("Found "+name+" Hence Skipping insertion case 2");
                    continue;
                }
                else
                {

                        Node parentNode = getParent(root, parent.name);
                        System.out.println("Parent is " + parentNode.getName());
                        parentNode.getChildren().add(n1);
                        continue;

                }

            }
            Node parentNode=null;
            if(parent.name!=null)
                parentNode=getParent(root, parent.name);

            if(parentNode!=null)
            {
                System.out.println("parent "+parentNode.name);
                parentNode.addChild(n);
                n.setParent(parentNode);
                TreeHash.put(rdd_no, n);
            }
        }

        Tree newTree = new Tree();
        newTree.roots.add(root);
        roots.add(newTree);
    return ;
    }

    public ArrayList<Node> getPreOrderTraversal(Tree tree) {

        ArrayList<Node> preOrder = new ArrayList<Node>();

        System.out.println("The number of roots in the tree is "+ tree.roots.size());
        for(int i = 0 ;i< tree.roots.size();i++) {

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
    void calculateCriticality()
    {
        calculateCriticalityNumber(root,0);
        int number=totalNodes(root);
        System.out.println("The total number of nodes is "+number);
        calculateCriticalityPercentage(root, number);
    }
    void calculateCriticalityPercentage(Node root, int number)
    {
        root.setCritic_percentage((double) root.criticality/ (double) number*100);
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
        root.criticality=critic;

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
public class CriticalityTreeNACRS {


    public static void main(String args[])
    {
        CTreeNACRS ctree = new CTreeNACRS();
        ctree.populateHashMap();
        ctree.parseLines("/home/aparna/FTspark/CriticalityTree/src/inputNACRS");
        ctree.root = null;
        ctree.processLines();    ctree.Lines.clear();
     //   ctree.calculateCriticality();
        ctree.parseLines("/home/aparna/FTspark/CriticalityTree/src/InputNACRS1");

        ctree.root = null;
        ctree.processLines();
        System.out.println("******** GetPreOrder **********");
        ArrayList<Node> preOrder = new ArrayList<Node>();
        int i;

        System.out.println("Number fo trees is " + ctree.roots.size());
        for( int j =0 ; j < ctree.roots.size(); j++) {


            preOrder = ctree.getPreOrderTraversal(ctree.roots.get(j));

            for (i = 0; i < preOrder.size(); i++) {
                System.out.println(preOrder.get(i).getName() + " " + preOrder.get(i).getRdd_no());
            }
        }
        //ctree.print();

    }
}
