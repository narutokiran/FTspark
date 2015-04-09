package FTSparkDriver;

/**
 * Created by aparna on 08/04/2015
 */

import java.util.ArrayList;
import java.util.List;

public class Node {

    private int line_no;
    private String name;
    private int criticality;
    private List<Node> children;
    private Node parent;
    private int no_spaces;
    private double critic_percentage;

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

    public void setCriticality(int critic)
    {
        criticality=critic;
    }
    public void setCritic_percentage(double critic)
    {
        critic_percentage=critic;
    }
}