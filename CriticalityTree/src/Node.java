/**
 * Created by aparna on 27/02/15.
 */

import java.util.ArrayList;
import java.util.List;

public class Node {

        int line_no;
        String name;
        int criticality;
        List<Node> children;
        Node parent;
        int no_spaces;
        double critic_percentage;

        Node(int line_no, String name, int spaces)
        {
            System.out.println("Constructing Node with "+line_no+" "+name+" "+spaces);
            this.line_no=line_no;
            this.name=name;
            this.children=new ArrayList<Node>();
            this.no_spaces=spaces;
        }

        public void addChild(Node child)
        {
            child.setParent(this);
            children.add(child);
        }

        public void setParent(Node parent)
        {
            this.parent = parent;
        }

        public List<Node> getChildren()
        {
            return this.children;
        }

        public int getNo_spaces()
        {
            return no_spaces;
        }
    public String getName()
    {
        return name;
    }
    public int getCriticality()
    {
        return criticality;
    }
    public double getCritic_percentage()
    {
        return critic_percentage;
    }
}