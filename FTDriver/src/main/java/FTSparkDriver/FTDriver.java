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

    public FTDriver(persistRDDs workflow)
    {
        System.out.println("Initializing Fault Tolerant Driver");
        workflow.persist("lines1");
    }
}
