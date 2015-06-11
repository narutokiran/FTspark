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
    private List<Node> parent;
    private int no_spaces;
    private double critic_percentage;
    private boolean isStage;
    int rdd_no;
    private boolean setTime;

    Node(int line_no, String name, int spaces, boolean isStage)
    {
        System.out.println("Constructing Node with "+line_no+" "+name+" "+spaces);
        this.line_no=line_no;
        this.name=name;
        this.children=new ArrayList<Node>();
        this.no_spaces=spaces;
        this.isStage=isStage;
        this.setTime= false;
    }

    Node(int line_no, String name, int spaces, boolean isStage, int rdd_no)
    {
        System.out.println("Constructing Node with "+line_no+" "+name+" "+spaces+" "+rdd_no);
        this.line_no=line_no;
        this.name=name;
        this.children=new ArrayList<Node>();
        this.parent = new ArrayList<Node>();
        this.no_spaces=spaces;
        this.rdd_no=rdd_no;
        this.isStage=isStage;
        this.setTime=false;
    }


    public void addChild(Node child)
    {
        child.addParent(this);
        children.add(child);
    }
    public List<Node> getParents()
    {
        return this.parent;
    }
    public void addParent(Node p)
    {
        parent.add(p);
    }


    public List<Node> getChildren()
    {

        return this.children;
    }

    public void setIsStage(boolean isStage){
        this.isStage=isStage;
    }
    public boolean getIsStage() {
        return isStage;
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
    public int getLine_no() {
        return line_no;
    }
    public int getRdd_no() {
        return rdd_no;
    }
    public void setRdd_no(int no) {
        rdd_no=no;
    }
    public boolean isSetTime()
    { return setTime; }
    public void setSetTime(boolean set) { setTime=set; }
}

class Tree
{
    ArrayList<Node> roots;

    Tree()
    {
        roots = new ArrayList<Node>();
    }
}