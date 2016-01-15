package Canopy;

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
		//System.out.println(key.toString());
		for(ClusterCenter valueCenter : values)
		{

			boolean isClose=false;

			for (ClusterCenter centerList : canopyCenters)
			{
				//remove him from the list.
				double distance = DistanceMeasurer.measureDistance(centerList, valueCenter.getCenter());
				if ( distance <= DistanceMeasurer.T1 ) //almost the same
				{
					isClose = true;
					if(distance > DistanceMeasurer.T2)
					{
						//System.out.println(centerList+" : "+ centerList.getCenter().getNeighbors());
						centerList.getCenter().setNeighbors(centerList.getCenter().getNeighbors()+1);
						//center.setNeighbors(center.getNeighbors()+1);
					}
					break;

				}
			}
			if (!isClose)
			{
				//System.out.println("first Center:"+ valueCenter);
				ClusterCenter stockvector = new ClusterCenter(valueCenter);
				canopyCenters.add(stockvector);
				//valueCenter.getCenter().setNeighbors(0);
			}	
		}
	}
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException
	{
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path canopy = new Path("/home/training/Desktop/Kmeans/canopyCenters.seq");
		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs,context.getConfiguration(), canopy , ClusterCenter.class,DoubleWritable.class);
		for (ClusterCenter center : canopyCenters)
		{
			System.out.println("Center neighbors:"+ center.getCenter().getNeighbors());
			System.out.println(new Double(center.getCenter().getNeighbors())/new Double(numberOfVectors));
			double finish = new Double(center.getCenter().getNeighbors())/new Double(numberOfVectors);
			int value = center.getCenter().getNeighbors();
			System.out.println(value);
			centerWriter.append(center,new DoubleWritable(finish));
			context.write(center,new DoubleWritable(finish));
		}
		centerWriter.close();
	}
}
