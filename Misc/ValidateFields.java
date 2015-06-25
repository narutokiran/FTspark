import java.io.*;

class ValidateFields
{
public static boolean checkInteger(String integer)
    {
        int i;

        boolean seenDigit=false;

        for(i=0;i<integer.length();i++)
        {

            char c = integer.charAt(i);

            if(c>='0' && c<='9')
            {
                seenDigit=true;
                continue;
            }
            return false;
        }
        return seenDigit;
    }

    public static boolean checkDouble(String DoubleString)
    {
        int i;

        boolean seenDigit= false;
        boolean seenDot = false;
        for(i=0; i<DoubleString.length();i++)
        {
            char c = DoubleString.charAt(i);

            if(c >= '0' && c<='9')
            {
                seenDigit=true;
                continue;
            }
            else if(c=='.' && !seenDot)
            {
                seenDot=true;
                continue;
            }
            return false;
        }
        return seenDigit;
    }
public static void main(String args[]) throws Exception
{
FileReader fr = new FileReader("/home/aparna/Desktop/FinalPredictionNACRS.csv");
BufferedReader br = new BufferedReader(fr);
String line;
int count=0;
while((line=br.readLine())!=null)
{

String temp[] = line.split(",");                                                                                                                                                                              
if(temp.length!=26)
System.out.println("Length is not 26 in Line "+count);

for(int i=0;i<26;i++)
{
if(i==19)
{
boolean result = checkDouble(temp[i]);
            if(!result) {
System.out.println(line);
                         //  System.out.println(" The key is "+t2._1()+" and the value is "+temp[i]);
				System.out.println("Line "+ count+" DOes not contain Double values");
                           System.out.println(line);
		
                       }
}
else
{
boolean result = checkInteger(temp[i]);

            if(!result) {
System.out.println(line);
                         //  System.out.println(" The key is "+t2._1()+" and the value is "+temp[i]);
				System.out.println("Line "+ count+" DOes not contain Integer values");
                           System.out.println(line);
			
                       }
}
count++;
}

}
}
}
