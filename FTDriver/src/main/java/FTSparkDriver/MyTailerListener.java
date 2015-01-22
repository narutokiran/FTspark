package FTSparkDriver;
import org.apache.commons.io.input.TailerListenerAdapter;
/**
 * Created by aparna on 22/01/15.
 */
public class MyTailerListener extends TailerListenerAdapter{

    public void handle(String Line)
    {
        System.out.println("***"+Line+"***");
    }



}
