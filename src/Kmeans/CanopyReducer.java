package Kmeans;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import Vector.ClusterCenter;
import Vector.DistanceMeasurer;


// calculate a new clustercenter for these vertices
public class CanopyReducer extends
		Reducer<IntWritable,ClusterCenter,ClusterCenter, DoubleWritable> {

	List<ClusterCenter> canopyCenters;
	int numberOfVectors;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		this.canopyCenters = new LinkedList<ClusterCenter>();
	}

	@Override
	protected void reduce(IntWritable key, Iterable<ClusterCenter> values,
			Context context) throws IOException, InterruptedException {
		numberOfVectors=new Integer(key+"");
		for(ClusterCenter valueCenter : values)
		{

			boolean isClose=false;

			for (ClusterCenter centerList : canopyCenters)
			{
				
				double distance = DistanceMeasurer.measureDistance(centerList, valueCenter.getCenter());
				if ( distance <= DistanceMeasurer.T1 ) 
				{
					isClose = true;
					if(distance > DistanceMeasurer.T2)
					{
						centerList.getCenter().setNeighbors(centerList.getCenter().getNeighbors()+1);
					}
					break;

				}
			}
			if (!isClose)
			{
				ClusterCenter stockvector = new ClusterCenter(valueCenter);
				canopyCenters.add(stockvector);
			}	
		}
	}
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException
	{
		super.cleanup(context);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path canopy = new Path("/home/training/Desktop/Kmeans/canopyCenters.seq");
		context.getConfiguration().set("canopy.path",canopy.toString());
		
		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs,context.getConfiguration(), canopy , ClusterCenter.class,DoubleWritable.class);
		for (ClusterCenter center : canopyCenters)
		{
			double finish = new Double(center.getCenter().getNeighbors())/new Double(numberOfVectors);
			int value = center.getCenter().getNeighbors();
			centerWriter.append(center,new DoubleWritable(finish));
			context.write(center,new DoubleWritable(finish));
		}
		centerWriter.close();	
		
	}
}
