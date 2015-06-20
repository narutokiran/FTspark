/**
 * Created by aparna on 05/06/15.
 */
import com.google.common.base.Stopwatch;
import java.io.*;
import java.util.*;

class IPs
{
    String HostName;
    String host;
    boolean status;


    IPs()
    {
        status = true;// meaning the machine is on
        host="ubuntu";
    }
}
class IPProcessing
{
  final ArrayList<IPs> ips;
    final HashMap<String, Integer> hm = new HashMap<String,Integer>();

    IPProcessing()
    {
        ips = new ArrayList<IPs>();
        hm.put("count",0);
    }

    void ProcessIps(String ipFile)
    {
        try
        {
            FileReader fr = new FileReader(ipFile);

            BufferedReader br = new BufferedReader(fr);

            String line;

           // String cmd="ssh ubuntu@";

            while((line=br.readLine())!=null)
            {
                IPs temp = new IPs();

                temp.HostName= line;

                ips.add(temp);


             /*   try {
                    // Execute command
                    String command = "ssh ubuntu@ec2-52-11-76-83.us-west-2.compute.amazonaws.com";
                    Process child = Runtime.getRuntime().exec(command);

                    // Get output stream to write from it
                    OutputStream out = child.getOutputStream();

                    out.write("sudo shutdown -h now".getBytes());
                    out.flush();
                    //out.write("dir /r/n".getBytes());
                    out.close();
                } catch (IOException e) {
                }*/

            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public void TimerOn() {


        final Timer timer = new Timer();
        System.out.println("Timer created");
        timer.schedule(new TimerTask() {
            public void run() {
                int count = hm.get("count");
                System.out.println("count is "+count);
                count = count + 1;

                hm.put("count", count);
                if (count == ips.size()+1)
                {
                    timer.cancel();
                }
                count=count-1;
                String command = "ssh ubuntu@";
                String line=ips.get(count).HostName;
                command+=line;

                try {
                    System.out.println("creating process");
                    Process child = Runtime.getRuntime().exec(command);
                    OutputStream out = child.getOutputStream();
                    out.write("sudo shutdown -h now".getBytes());
                    out.flush();

                    out.close();
                /*   BufferedReader in = new BufferedReader(new InputStreamReader(child.getErrorStream()));
                    String s;

                    while((s=in.readLine())!=null)
                    {
                        System.out.println(s);
                    }*/



                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 80 *1000, 1 * 1000);
    }


}
public class MainProgram {

    public static void main(String args[])
    {
      //  Stopwatch stopwatch =Stopwatch.createStarted();

        IPProcessing ipProcessing = new IPProcessing();

        ipProcessing.ProcessIps("/home/aparna/FTspark/MainProgram/src/main/java/IPS.txt");

        ipProcessing.TimerOn();

       // System.out.println(stopwatch.stop());
    }
}
