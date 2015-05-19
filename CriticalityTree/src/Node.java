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
        int rdd_no;

        Node(int line_no, String name, int spaces)
        {
            System.out.println("Constructing Node with "+line_no+" "+name+" "+spaces);
            this.line_no=line_no;
            this.name=name;
            this.children=new ArrayList<Node>();
            this.no_spaces=spaces;
        }
    Node(int line_no, String name, int spaces, int rdd_no)
    {
        System.out.println("Constructing Node with "+line_no+" "+name+" "+spaces+" "+rdd_no);
        this.line_no=line_no;
        this.name=name;
        this.children=new ArrayList<Node>();
        this.no_spaces=spaces;
        this.rdd_no=rdd_no;
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

    public void setCriticality(int critic)
    {
        criticality=critic;
    }
    public void setCritic_percentage(double critic)
    {
        critic_percentage=critic;
    }
    public int getRdd_no() {
        return rdd_no;
    }
    public void setRdd_no(int no) {
        rdd_no=no;
    }
}