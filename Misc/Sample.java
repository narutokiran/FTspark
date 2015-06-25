import java.io.*;

class Sample
{
public static void main(String args[]) throws Exception
{

String line;
int count=0;

FileReader fr=new FileReader("FinalPredictionNACRS.csv");

BufferedReader br = new BufferedReader(fr);

FileWriter fw = new FileWriter("SampledFinalPrediction.csv");

BufferedWriter bw = new BufferedWriter(fw);


while((line=br.readLine())!=null)
{
count++;
if(count%10==0)
{
bw.write(line+"\n");
bw.flush();
}
}

}
}
