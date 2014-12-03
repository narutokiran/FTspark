/**
 * Created by aparna on 03/12/14.
 */
import java.io.*;


public class MainDriver{

        static WorkFlowInput input;

        public static void main (String argv []) throws Exception
        {
                /* variable declarations */
                String InputXMLFile="";    /* variable that holds the input file format */
                int i;
                int flagi=0;  /* flagi -> flag for input */

                /* Reading the input workflow file from command line arguments */
                for(i=0;i<argv.length-1; i++) {

                    if(argv[i].equals("--input")) {
                        InputXMLFile = argv[i+1];
                        System.out.println("The Input FileName Containing Workflow is " + InputXMLFile);
                        i++;
                        flagi=1;
                    }
                }
                if(flagi==0)
                {
                    System.out.println("The arguments specified are incorrect. Please check below for proper usage \n " +
                        "--input \t Specifies the XML file where the Workflow details are specified \n");
                }

                /* Parsing the XML file and obtaining the input format */
                input=new WorkFlowInput(InputXMLFile);
                input.parseXML();
                input.printVariables();



        }
}

