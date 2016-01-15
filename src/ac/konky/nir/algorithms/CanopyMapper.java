package ac.konky.nir.algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ac.konky.nir.vector.ClusterCenter;
import ac.konky.nir.vector.DistanceMeasurer;
import ac.konky.nir.vector.Vector;

public class CanopyMapper extends Mapper<LongWritable, Text, IntWritable, ClusterCenter> {

	List<ClusterCenter> centers;
	int numberOfVectors;
	int vectorSize;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.centers = new LinkedList<ClusterCenter>();
		Configuration conf = context.getConfiguration();

		

		
		this.vectorSize = new Integer(conf.get("vectorSize")).intValue();
		
		System.out.println("Canopy Mapper");
	}

	@Override
	protected void map(LongWritable key,Text value, Context context)throws IOException, InterruptedException 
	{
		ArrayList<String> Data = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
		for (int i = 1; i < Data.size()-vectorSize; i+=vectorSize) 
		{
			double[] doubleArray = new double[vectorSize];
			for (int j = 0; j < vectorSize; j++) {
				doubleArray[j] = new Double(Data.get(i+j).substring(1, Data.get(i+j).length()-1));
			}
			
			
			Vector newVector = new Vector(doubleArray);
			numberOfVectors++;
			boolean isClose = false;
			for (ClusterCenter center : centers)
			{
				double distance = DistanceMeasurer.measureDistance(center, newVector);
				if ( distance<= DistanceMeasurer.T1 ) 
				{
					isClose = true;
		         				 
					if(distance > DistanceMeasurer.T2)
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

	}
	
}

