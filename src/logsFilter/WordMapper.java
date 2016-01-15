package logsFilter;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class WordMapper extends Mapper<LongWritable, Text, logWritable, IntWritable> {

	  private logWritable log = new logWritable();
	
  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] splitedstring = line.split(" ");
    Text rowID = new Text();
    Text Request = new Text();
    String stringtoTimeStamp= "";
    Text TimeStamp = new Text();
    Text userName = new Text();
    
   
   /*
    line.trim();

    while(line.contains("  ")&&line.length()>0)
    {
     int index = line.indexOf("  ");
     line  = line.substring(0,index)+line.substring(index+1);
    }
    String[] splitedString = line.split(" ");
    rowID = new Text(splitedString[0]);
    Request = new Text(splitedString[1]);
    TimeStamp = new Text(splitedString[2]+" "+splitedString[3]+" "+splitedString[4]);
    userName = new Text(splitedString[5]);
    
    */
    int counter = 0;
    for(String a : splitedstring)
    {
    	if (a.isEmpty())
    	{
    		//DOnt do nothing.
    	}
    	else{
    		if (counter==0)
    		{
    			rowID = new Text(a.trim());
    			counter++;
    		}
    		else if(counter==1){
    			Request = new Text(a.trim());
    			counter++;
    		}
    		else if (counter==2 || counter ==3 || counter ==4){
    			stringtoTimeStamp+= a+" ";
    			counter++;
    		}
    		else if(counter==5){
    			userName = new Text(a.trim());
    		}
    	}
    }
    TimeStamp = new Text(stringtoTimeStamp.trim());
        log.setRowID(rowID);
        log.setRequest(Request);
        log.setTimestamp(TimeStamp);
        log.setUserName(userName);


    context.write(log, new IntWritable(1));
        }
        
      
    }
  