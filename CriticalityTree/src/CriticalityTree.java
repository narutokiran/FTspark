/**
 * Created by aparna on 27/02/15.
 */
import java.io.*;
import java.util.HashMap;
class CTree
{
    HashMap<Integer, String> hm = new HashMap<Integer, String>();
    int no_lines;
    String lines[]=new String[100];
    Node root=null;

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
                lines[i++]=line;
            }
            no_lines=i;
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    void print()
    {
        int i;
        for(i=0;i<no_lines;i++)
        {
            System.out.println(lines[i]);
        }
    }
    void processLines()
    {
        int i;
        for(i=no_lines-1;i>=0;i--)
        {
            /* counting Space */
                String t=lines[i];
                t=t.replaceAll("( )+"," ");
                System.out.println(t);
                String temp[]=t.split(" ");
                int count_spaces=0;
                while(!Character.isLetter(lines[i].charAt(count_spaces)))
                {
                    count_spaces++;
                }

                for(int j=0;j<temp.length;j++)
                {
                    System.out.println(temp[j]);
                }
                //String[] t = line.split(":");
            //    int line_no=Integer.parseInt(t[1]);
                System.out.println(count_spaces+" ");



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
