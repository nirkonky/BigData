package Canopy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import Vector.ClusterCenter;
import Vector.DistanceMeasurer;
import Vector.Vector;

public class CanopyMapper extends Mapper<LongWritable, Text, IntWritable, ClusterCenter> {

	List<ClusterCenter> centers;
	int numberOfVectors;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.centers = new LinkedList<ClusterCenter>();
		int numberOfVectors = 0;
		
	}

	@Override
	protected void map(LongWritable key,Text value, Context context)throws IOException, InterruptedException 
	{
		
		ArrayList<String> Data = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
		for (int i = 1; i < Data.size()-4; i+=4) 
		{
			double[] doubleArray = new double[4];
			doubleArray[0] = new Double(Data.get(i));
			doubleArray[1] = new Double(Data.get(i+1));
			doubleArray[2] = new Double(Data.get(i+2));
			doubleArray[3] = new Double(Data.get(i+3));
			
			Vector newVector = new Vector(doubleArray);
			numberOfVectors++;
			boolean isClose = false;
			//T2 smallest T1 bigger
			for (ClusterCenter center : centers)
			{
				double distance = DistanceMeasurer.measureDistance(center, newVector);
				//remove him from the list.
				if ( distance<= DistanceMeasurer.T1 ) //almost the same
				{
					isClose = true;
		         				 
					//bitween T2 and T2;
					if(distance <= DistanceMeasurer.T1)
					{
						center.getCenter().setNeighbors(center.getCenter().getNeighbors()+1);
					}
					break;
				}
			
			}
			if (!isClose)
			{
			   	ClusterCenter center = new ClusterCenter(newVector);
		        centers.add(center);
		        center.getCenter().setNeighbors(1);
		    }
		}
	}
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException
	{
		super.cleanup(context);
		for (ClusterCenter center : centers)
		{
			context.write(new IntWritable(numberOfVectors),center);
		}
		//System.out.println("number of vectors:"+ numberOfVectors);

	}
	
}

