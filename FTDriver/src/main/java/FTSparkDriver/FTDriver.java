package FTSparkDriver;
import java.io.*;
import org.apache.spark.api.java.JavaSparkContext;
import japa.parser.JavaParser;
import japa.parser.ast.CompilationUnit;
import japa.parser.ast.visitor.VoidVisitorAdapter;
import japa.parser.ast.body.MethodDeclaration;
/**
 * Created by aparna on 04/12/14.
 */
public class FTDriver {

   /* static WorkFlowInput input;
    static FTSparkAlgorithm FTalgo;
    static int flagi=0;
    static JavaSparkContext sparkContext;*/
    /* Constructor */
    /*public FTDriver(JavaSparkContext sparkContext, String inputXMLFile)
    {
        System.out.println("------ Created Driver Instance -----");
        this.sparkContext=sparkContext;
        System.out.println("------ Both are pointing to the same Spark Configuration ------");
        setInputXMLFile(inputXMLFile);

    }*/

    /*public void setInputXMLFile(String inputXMLFile)
    {
        flagi=1;
        input=new WorkFlowInput(inputXMLFile);
        input.parseXML();
        input.printVariables();
    }*/

    public static void main(String args[]) throws Exception
    {
        FileInputStream in = new FileInputStream("/home/aparna/FTspark/WordCountWorkFlow/src/main/java/WordCountWorkFlow.java");

        CompilationUnit cu;
        try {
            // parse the file
            cu = JavaParser.parse(in);
        } finally {
            in.close();
        }

        // prints the resulting compilation unit to default system output
       // System.out.println(cu.toString());
        new MethodVisitor().visit(cu, null);
    }
    private static class MethodVisitor extends VoidVisitorAdapter {

        @Override
        public void visit(MethodDeclaration n, Object arg) {
            // here you can access the attributes of the method.
            // this method will be called for all methods in this
            // CompilationUnit, including inner class methods
            System.out.println(n.getName());
        }
    }

}
