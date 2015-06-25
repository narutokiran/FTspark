/* this file is used to perform all the functions performed inside the rdd! */

import java.io.*;
import java.util.HashMap;
import java.util.Map;
class Validate
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
String line;
FileReader fr = new FileReader("FinalNACRS.csv");
BufferedReader br= new BufferedReader(fr);

FileWriter fw = new FileWriter("PredictionValidatedNACRS.csv");
BufferedWriter bw = new BufferedWriter(fw);
         Map<String, Integer> Mapping0 = new HashMap<String, Integer>();
	Map<String, Integer> Mapping1 = new HashMap<String, Integer>();
Map<String, Integer> Mapping2 = new HashMap<String, Integer>();
Map<String, Integer> Mapping3 = new HashMap<String, Integer>();
Map<String, Integer> Mapping5 = new HashMap<String, Integer>();
Map<String, Integer> Mapping10 = new HashMap<String, Integer>();
Map<String, Integer> Mapping13 = new HashMap<String, Integer>();
Map<String, Integer> Mapping15 = new HashMap<String, Integer>();
Map<String, Integer> Mapping16 = new HashMap<String, Integer>();
Map<String, Integer> Mapping17 = new HashMap<String, Integer>();
Map<String, Integer> Mapping18 = new HashMap<String, Integer>();
Map<String, Integer> Mapping19 = new HashMap<String, Integer>();
Map<String, Integer> Mapping22 = new HashMap<String, Integer>();
Map<String, Integer> Mapping23 = new HashMap<String, Integer>(); 
Map<String, Integer> Mapping25 = new HashMap<String, Integer>();
Map<String, Integer> Mapping26 = new HashMap<String, Integer>();
int count[] = new int[27];

        // Acumulator????
        //

        
	while((line=br.readLine())!=null)
	{
	String[] temp= line.split(",");
	
	int flag=0;
	/* check length */
	
	if(temp.length!=27)
	{
	System.out.println("rejected for length");
	System.out.println(line);
	flag=1;
	}
	if(flag==1)
	continue;
	for(int i =0 ; i <27; i++)
	{
	if(temp[i].equals("NULL"))
	{
	System.out.println("rejected bcoz it contained NULL values");
	System.out.println(line);
	flag=1;
	}
	}
	
	 for(int i=0; i < temp.length ;i++)
               {

                   if(i==4 || i==6 || i==7 || i==8 || i==9 || i==11 || i==12 || i==14 || i==21 || i==24)
                   {
                        boolean result = checkInteger(temp[i]);
                       if(!result) {
                         //  System.out.println(" The key is "+t2._1()+" and the value is "+temp[i]);
				System.out.println("Rejecetd coz it did not contain Integer values");
                           System.out.println(line);
			flag=1;
                       }
                   }
                   else if(i==20)
                   {
                       boolean result = checkDouble(temp[i]);
                       if(!result){

			System.out.println("Rejecetd coz it did not contain Integer values");
                        System.out.println(line);
			flag=1;
                       }
                   }

               }
if(flag==1)
continue;	
bw.write(line+"\n");
bw.flush();
	
	}

FileReader fr1 = new FileReader("PredictionValidatedNACRS.csv");
br = new BufferedReader(fr1);

FileWriter fw1 = new FileWriter("FinalPredictionNACRS.csv");
bw = new BufferedWriter(fw1);

for(int i=0;i<27;i++)
count[i]=0;
while((line=br.readLine())!=null)
{
int num=0;
	String temp[] = line.split(",");
	
	for(int i=0;i<temp.length;i++)
	{
	switch(i)
	{
	
	case 1:
	if(!Mapping1.containsKey(temp[1]))
	{
	Mapping1.put(temp[1], count[i]+1);
	num= count[i]+1;
	count[1]++;
	}
	else
	{
	num=Mapping1.get(temp[1]);
	}
	temp[1]=Integer.toString(num);
	break;
	case 2:
	if(!Mapping2.containsKey(temp[2]))
	{
	Mapping2.put(temp[2], count[i]+1);
	num=count[2]+1;
	count[2]++;
	}
	else
	{
	num=Mapping2.get(temp[2]);
	}
	temp[2]=Integer.toString(num);
	break;
	case 3:
	if(!Mapping3.containsKey(temp[3]))
	{
	Mapping3.put(temp[3], count[i]+1);
	num=count[3]+1;
	count[3]++;
	}
	else
	{
	num=Mapping3.get(temp[3]);
	}
temp[3]=Integer.toString(num);
	break;
	case 5:
	if(!Mapping5.containsKey(temp[5]))
	{
	Mapping5.put(temp[5], count[i]+1);
	num=count[5]+1;
	count[5]++;
	}
	else
	{
	num=Mapping5.get(temp[5]);
	}
temp[5]=Integer.toString(num);
	break;
	case 10:
	if(!Mapping10.containsKey(temp[10]))
	{
	Mapping10.put(temp[10], count[i]+1);
	num=count[10]+1;
	count[10]++;
	}
	else
	{
	num=Mapping10.get(temp[10]);
	}
temp[10]=Integer.toString(num);
	break;
	case 13:
		if(!Mapping13.containsKey(temp[13]))
	{
	Mapping13.put(temp[13], count[i]+1);
	num=count[13]+1;
	count[13]++;
	}
	else
	{
	num=Mapping13.get(temp[13]);
	}
temp[13]=Integer.toString(num);
	break;
	case 15:
	if(!Mapping15.containsKey(temp[15]))
	{
	Mapping15.put(temp[15], count[i]+1);
	num=count[15]+1;
	count[15]++;
	}
	else
	{
	num=Mapping15.get(temp[15]);
	}
temp[15]=Integer.toString(num);
break;
	case 16:
	if(!Mapping16.containsKey(temp[16]))
	{
	Mapping16.put(temp[16], count[i]+1);
	num=count[16]+1;
	count[16]++;
	}
	else
	{
	num=Mapping16.get(temp[16]);
	}
temp[16]=Integer.toString(num);
break;
	case 17:
	if(!Mapping17.containsKey(temp[17]))
	{
	Mapping17.put(temp[17], count[i]+1);
	num=count[17]+1;
	count[17]++;
	}
	else
	{
	num=Mapping17.get(temp[17]);
	}
temp[17]=Integer.toString(num);
break;
	case 18:
	if(!Mapping18.containsKey(temp[18]))
	{
	Mapping18.put(temp[18], count[i]+1);
	num=count[18]+1;
	count[18]++;
	}
	else
	{
	num=Mapping18.get(temp[18]);
	}
temp[18]=Integer.toString(num);
break;
	case 19:
	if(!Mapping19.containsKey(temp[19]))
	{
	Mapping19.put(temp[19], count[i]+1);
	num=count[19]+1;
	count[19]++;
	}
	else
	{
	num=Mapping19.get(temp[19]);
	}
temp[19]=Integer.toString(num);
break;
	case 22:
	if(!Mapping22.containsKey(temp[22]))
	{
	Mapping22.put(temp[22], count[i]+1);
	num=count[22]+1;
	count[22]++;
	}
	else
	{
	num=Mapping22.get(temp[22]);
	}
temp[22]=Integer.toString(num);
break;
	case 23:
	if(!Mapping23.containsKey(temp[23]))
	{
	Mapping23.put(temp[23], count[i]+1);
	num=count[23]+1;
	count[23]++;
	}
	else
	{
	num=Mapping23.get(temp[23]);
	}
temp[23]=Integer.toString(num);
break;
	case 25:
	if(!Mapping25.containsKey(temp[25]))
	{
	Mapping25.put(temp[25], count[i]+1);
	num=count[25]+1;
	count[25]++;
	}
	else
	{
	num=Mapping25.get(temp[25]);
	}
temp[25]=Integer.toString(num);
break;
	case 26:
	if(!Mapping26.containsKey(temp[26]))
	{
	Mapping26.put(temp[26], count[i]+1);
	num=count[26]+1;
	count[26]++;
	}
	else
	{
	num=Mapping26.get(temp[26]);
	}
temp[26]=Integer.toString(num);
break;
	}
	} 
String res="";
for(int i=1;i<temp.length-1;i++)
{
res+=temp[i];
res+=",";
}
res+=temp[temp.length-1];
bw.write(res+"\n");
bw.flush();

}
}

}
