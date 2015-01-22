import java.io.*;
class FileData
{
String [] Filelines;
String FileName;
int countLines;

FileData(int n,String FileName)
{
countLines=n;
Filelines=new String[n+2];
this.FileName=FileName;
}

void populate () throws Exception
{
int count=0;
File fl=new File(FileName);
BufferedReader br=new BufferedReader(new FileReader(fl));
String line=br.readLine();
while(line!=null)
{
Filelines[count++]=line;
line=br.readLine();
}
}

void print()
{
int i;
for(i=0;i<countLines;i++)
System.out.println(i+" "+Filelines[i]);
}

}
public class ParseFile
{
static int countlines(String filename)
{
int lineCount=0;
try{
Process p=Runtime.getRuntime().exec("wc -l "+filename);
p.waitFor();
BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream()));
String line="";
String temp[];
while((line=br.readLine())!=null)
{
System.out.println(line);
temp=line.split(" ");
lineCount=Integer.parseInt(temp[0]);
}
}
catch(Exception e)
{
e.printStackTrace();
}
return lineCount;
}

public static void main(String args[]) throws Exception
{
int n=countlines("src/main/java/WordCountWorkFlow.java");
FileData fd=new FileData(n,"src/main/java/WordCountWorkFlow.java");
fd.populate();
fd.print();
}
}
