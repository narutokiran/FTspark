import java.io.*;
class Parse
{

public static void main(String args[]) throws Exception
{

FileReader fr = new FileReader("NACRS_export4.txt");

BufferedReader br = new BufferedReader(fr);

FileWriter fw = new FileWriter("FinalNACRS.csv");

BufferedWriter bw = new BufferedWriter(fw);

String line;

while((line=br.readLine())!=null)
{

	String temp[] = line.split("\\|");

//Writing keys
	//bw1.write(temp[0]+"\n");
	//bw1.flush();

	String l="";
	for(int i=0; i< temp.length-1; i++)
	{
	if(!temp[i].equals("\"\""))
	{
	temp[i]=temp[i].substring(1);
	temp[i]=temp[i].substring(0, temp[i].length()-1);
	}
	else
	{
temp[i]="NULL";
	}
	temp[i]=temp[i].trim();
	l+=temp[i]+",";
	}
	temp[temp.length-1]=temp[temp.length-1].substring(1);
	temp[temp.length-1]=temp[temp.length-1].substring(0, temp[temp.length-1].length()-1);
	l+=temp[temp.length-1];
	bw.write(l+"\n");
	bw.flush();

}


}

}
