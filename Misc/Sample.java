import java.io.*;

class Sample
{
public static void main(String args[]) throws Exception
{

String line;
int count=0;

FileReader fr=new FileReader("NACRS_removed_Null_field.txt");

BufferedReader br = new BufferedReader(fr);

FileWriter fw = new FileWriter("SampledNACRS.txt");

BufferedWriter bw = new BufferedWriter(fw);


while((line=br.readLine())!=null)
{
count++;
if(count%10000==0)
{
bw.write(line+"\n");
bw.flush();
}
}

}
}
