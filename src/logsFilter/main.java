package logsFilter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class main {
	
	public static void main(String[] args) throws Exception {
		
		
		
		BufferedReader fileReader = new BufferedReader(new FileReader(new File("/home/training/Desktop/logFiles/Log0.txt")));
		String word="";
		//DataInputStream dis = new DataInputStream(new FileInputStream(new File("/home/training/Desktop/logFiles/Log0.txt")));
		
		
		word = fileReader.readLine().replaceAll("     ", "");
		//word.replaceAll("               ", " ");
		//word.replaceAll(" ","");
		//word = word.replace("\n", "").replace("\r", "");
		System.out.println(word);
		for(String word2 : fileReader.readLine().split(" "))
		{
			for(String b : word2.split(" "))
			{
				if(b.equals(" "))
				{
					
				}
				else
				{
					//System.out.println(b);
				}
			}
		}
		
		//System.out.println(word.toString());
		//logWritable l = new logWritable();
		//InputStream is = new ByteArrayInputStream(word.getBytes());
		//l.readFields(new DataInputStream(is));
		//System.out.println(l.toString());
		
		
	}

}
