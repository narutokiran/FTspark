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
    Node root=null;
    HashMap<Integer, List<lines>> dependencies = new HashMap<Integer, List<lines>>();

    /* Populate HashMap for creating tree */
    void populateHashMap()
    {
        hm.put(Integer.valueOf(244),"count7");
        hm.put(Integer.valueOf(241),"unions3");
        hm.put(211,"count5");
        hm.put(228,"count6");
        hm.put(207,"union1");
        hm.put(98,"count1");
        hm.put(90,"ones1");
        hm.put(82,"words1");
        hm.put(76,"input1");
        hm.put(130,"count2");
        hm.put(122,"ones2");
        hm.put(114,"words2");
        hm.put(111,"input2");
        hm.put(224,"union2");
        hm.put(163,"count3");
        hm.put(155,"ones3");
        hm.put(147,"words3");
        hm.put(144,"input3");
        hm.put(194,"count4");
        hm.put(178,"words4");
        hm.put(175,"input4");
        hm.put(186,"ones4");

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
    boolean check(Node root, String name)
    {
        boolean found=false;

        if(root.name.equals(name))
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
            String name = hm.get(l);

            temp_line.operation=temp[length-3];

            temp_line.l_no=l;

            temp_line.name=name;

            Node n=new Node(l,name, count_spaces);
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
                System.out.println("parent "+parentNode.name);
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
}
