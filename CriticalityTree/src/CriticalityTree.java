/**
 * Created by aparna on 27/02/15.
 */
import java.io.*;
import java.util.HashMap;

import java.util.ArrayList;
import java.util.List;

class lines
{
    String line;
    String name;
    String operation;
    int l_no;
}

class Dependency
{
    lines line_object;
    int children;

}

class CTree
{
    HashMap<Integer, String> hm = new HashMap<Integer, String>();
    int no_lines;
    List<lines> Lines=new ArrayList();
    Node root=null;
    List<Dependency> dependencies = new ArrayList();

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
                System.out.println(t);

                String temp[]=t.split(" ");

                /* COunting the number of characters in the line */
                int count_spaces=0;
                while(!Character.isLetter(temp_line.line.charAt(count_spaces)))
                {
                    count_spaces++;
                }

                for(int j=0;j<temp.length;j++)
                {
                    System.out.println(j+" "+temp[j]);
                }
                System.out.println(count_spaces+" ");

                int length=temp.length;

                String t1[] = temp[length-1].split(":");
                int l=Integer.parseInt(t1[1]);
                String name = hm.get(l);
                temp_line.operation=temp[3];
                temp_line.l_no=l;
                temp_line.name=name;

                Node n=new Node(l,name, count_spaces);


                /* Insering into the tree */

                /* iF No Root , Make it root and continue to next line */
                if(root==null) {
                    root = n;
                    continue;
                }
               // System.out.println(root.name);
                lines parent = Lines.get(i-1);
                System.out.println(parent.name);
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
        getPreOrderTraversal();

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


}
public class CriticalityTree {

    public static void main(String args[])
    {
        CTree ctree = new CTree();
        ctree.populateHashMap();
        ctree.parseLines("/home/aparna/FTspark/CriticalityTree/src/Input");
        //ctree.print();
        ctree.processLines();
    }
}
