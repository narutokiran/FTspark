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
   Node parent;
    private int no_spaces;
    private double critic_percentage;
    private boolean isStage;
    int rdd_no;

    Node(int line_no, String name, int spaces, boolean isStage)
    {
        System.out.println("Constructing Node with "+line_no+" "+name+" "+spaces);
        this.line_no=line_no;
        this.name=name;
        this.children=new ArrayList<Node>();
        this.no_spaces=spaces;
        this.isStage=isStage;
    }

    Node(int line_no, String name, int spaces, boolean isStage, int rdd_no)
    {
        System.out.println("Constructing Node with "+line_no+" "+name+" "+spaces+" "+rdd_no);
        this.line_no=line_no;
        this.name=name;
        this.children=new ArrayList<Node>();
        this.no_spaces=spaces;
        this.rdd_no=rdd_no;
        this.isStage=isStage;
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
}