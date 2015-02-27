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


        Node(int line_no, String name, int criticality, int spaces)
        {
            this.line_no=line_no;
            this.name=name;
            this.criticality=criticality;
            this.children=new ArrayList<Node>();
            this.no_spaces=spaces;
        }
        public void addChild(Node child) {
            child.setParent(this);
            children.add(child);
            }

        public void setParent(Node parent) {
            this.parent = parent;
        }
}