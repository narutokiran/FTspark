/**
 * Created by aparna on 03/12/14.
 *
 * Description: This class is used to store the contents of the input workflow necessary to submit the job , run it and persist it accordingly.
 */

import java.io.File;
import org.w3c.dom.Document;
import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

class WorkFlowInput {

    private String filename; /* Name of the file */
    private String WorkFlowName;/* Name of the workflow */
    private String ClassName; /* Name of the class. For spark --class */
    private String Master; /* Master type, yarn-client, yarn-cluster */
    private String JobJar;
    private String JobSrc;
    private boolean isSrc;
    private boolean isHierarchy;
    private String NameSparkContext;

    /* should implement workflow options */

    WorkFlowInput(String filename)
    {
        this.filename=filename;
        this.WorkFlowName="";
        this.ClassName="";
        this.Master="";
        this.JobJar="";
        this.JobSrc="";
        this.isSrc=false;
        this.isHierarchy =false;
        this.NameSparkContext="";
    }

    /* Setter Methods */
    void setWorkFLowName(String WorkflowName)
    {
        this.WorkFlowName=WorkflowName;

    }
    void setClassName(String ClassName)
    {
        this.ClassName=ClassName;

    }
    void setMaster(String Master)
    {
        this.Master=Master;

    }
    void setJobJar(String JobJar)
    {
        this.JobJar=JobJar;

    }
    void setJobSrc(String JobSrc)
    {
        this.JobSrc=JobSrc;

    }
    void setisSrc(boolean isSrc)
    {
        this.isSrc=isSrc;

    }
    void setisHierarchy(boolean isHierarchy)
    {
        this.isHierarchy=isHierarchy;

    }
    void setNameSparkContext(String NameSparkContext)
    {
        this.NameSparkContext=NameSparkContext;
    }

    /* Get methods */


    String getWorkFlowName()
    {
        return WorkFlowName;
    }
    String getClassName()
    {
        return ClassName;
    }
    String getMaster()
    {
        return Master;
    }
    String getJobJar()
    {
        return JobJar;
    }
    String getJobSrc()
    {
        return JobSrc;
    }
    boolean getisSrc()
    {
        return isSrc;
    }
    boolean getisHierarchy()
    {
        return isHierarchy;
    }
    String getNameSparkContext()
    {
        return NameSparkContext;
    }

    boolean IsSrc()
    {
        return isSrc;
    }
    boolean IsHieararchy()
    {
        return isHierarchy;
    }

    /* For Debugging purposes */
    void printVariables()
    {
    System.out.println("WorkFlowName "+getWorkFlowName());
    System.out.println("ClassName "+getClassName());
    System.out.println("Master "+getMaster());
    System.out.println("Job Jar "+getJobJar());
    System.out.println("Job Src "+getJobSrc());
    System.out.println("Name of Spark Context "+getNameSparkContext());
    }


    void parseXML()
    {
        System.out.println("---------------------------Parsing the InputXMLWorkflow File---------------------");
        try {
            DocumentBuilderFactory odbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder odb =  odbf.newDocumentBuilder();
            Document odoc = odb.parse (new File("Workflow.xml"));
            odoc.getDocumentElement().normalize ();
            System.out.println ("Root element of the doc is " + odoc.getDocumentElement().getNodeName());
            NodeList LOW = odoc.getElementsByTagName("workflow");
            Node FWF =LOW.item(0);
            if(FWF.getNodeType() == Node.ELEMENT_NODE)
            {
                Element firstWorkflowElement = (Element)FWF;

                NodeList WorkflowNameList = firstWorkflowElement.getElementsByTagName("workflow_name");
                Element WorkFlowNameElement = (Element)WorkflowNameList.item(0);
                NodeList textWorkflowNameList = WorkFlowNameElement.getChildNodes();
                setWorkFLowName(((Node)textWorkflowNameList.item(0)).getNodeValue().trim());

                NodeList ClassNameList = firstWorkflowElement.getElementsByTagName("class_name");
                Element ClassNameElement = (Element)ClassNameList.item(0);
                NodeList textClassNameList = ClassNameElement.getChildNodes();
                setClassName(((Node)textClassNameList.item(0)).getNodeValue().trim());

                NodeList MasterList = firstWorkflowElement.getElementsByTagName("master");
                Element MasterElement = (Element)MasterList.item(0);
                NodeList textMasterList = MasterElement.getChildNodes();
                setMaster(((Node)textMasterList.item(0)).getNodeValue().trim());

                NodeList JobJarList = firstWorkflowElement.getElementsByTagName("job_jar");
                Element JobJarElement = (Element)JobJarList.item(0);
                NodeList textJobJarList = JobJarElement.getChildNodes();
                setJobJar(((Node)textJobJarList.item(0)).getNodeValue().trim());

                NodeList JobSrcList = firstWorkflowElement.getElementsByTagName("job_src");
                Element JobSrcElement = (Element)JobSrcList.item(0);
                NodeList textJobSrcList = JobSrcElement.getChildNodes();
                setJobSrc(((Node)textJobSrcList.item(0)).getNodeValue().trim());

                NodeList NameSparkContextList = firstWorkflowElement.getElementsByTagName("name_sc");
                Element NameSparkContextElement = (Element)NameSparkContextList.item(0);
                NodeList textNameSparkContextList = NameSparkContextElement.getChildNodes();
                setNameSparkContext(((Node)textNameSparkContextList.item(0)).getNodeValue().trim());


                }    //end of if clause
        }catch (SAXParseException err) {
            System.out.println (err.getMessage ());
        }catch (SAXException e) {
            e.printStackTrace ();
        }catch (Throwable t) {
            t.printStackTrace ();
        }
    }



}
