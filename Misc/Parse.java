import java.io.*;
class Parse
{

public static void main(String args[]) throws Exception
{

FileReader fr = new FileReader("SampledNACRS.txt");

BufferedReader br = new BufferedReader(fr);

FileWriter fw = new FileWriter("ParsedNACRS.txt");

BufferedWriter bw = new BufferedWriter(fw);

FileWriter fw1 = new FileWriter("ParsedKeys.txt");

BufferedWriter bw1= new BufferedWriter(fw1);

String line;

while((line=br.readLine())!=null)
{

	String temp[] = line.split("\\|");

//Writing keys
	bw1.write(temp[0]+"\n");
	bw1.flush();

	String l="";
	for(int i=1; i< temp.length-1; i++)
	{
	l+=temp[i]+",";
	}

	l+=temp[temp.length-1];
	bw.write(l+"\n");
	bw.flush();

}


}

}
