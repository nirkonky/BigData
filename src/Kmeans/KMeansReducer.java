package Kmeans;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;


import Vector.CenterCentroidWritable;
import Vector.ClusterCenter;
import Vector.Vector;

// calculate a new clustercenter for these vertices
public class KMeansReducer extends
		Reducer<CenterCentroidWritable, Vector, ClusterCenter, Vector> {

	public static enum Counter {
		CONVERGED
	}

	List<ClusterCenter> centers = new LinkedList<ClusterCenter>();
	HashMap<ClusterCenter,Vector[]> cv = new HashMap<ClusterCenter,Vector[]>();
	@Override
	protected void reduce(CenterCentroidWritable key, Iterable<Vector> values,
			Context context) throws IOException, InterruptedException {

		Vector newCenter = new Vector();
		List<Vector> vectorList = new LinkedList<Vector>();
		int vectorSize = key.getCentroid().getVector().length;
		newCenter.setVector(new double[vectorSize]);
		for (Vector value : values) {
			vectorList.add(new Vector(value));
			for (int i = 0; i < value.getVector().length; i++) {
				newCenter.getVector()[i] += value.getVector()[i];
			}
		}
		
		for (int i = 0; i < newCenter.getVector().length; i++) {
			
			newCenter.getVector()[i] = newCenter.getVector()[i]
					/ vectorList.size();
		}
		

		ClusterCenter center = new ClusterCenter(newCenter);
		centers.add(center);
		
		for (Vector vector : vectorList) {
			context.write(center, vector);
			if(cv.containsKey(center)){
			}
		}

		if (center.converged(new ClusterCenter(key.getCentroid())))
			context.getCounter(Counter.CONVERGED).increment(1);
		System.out.println("Number of centers: "+centers.size());
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		final SequenceFile.Writer out = SequenceFile.createWriter(fs,
				context.getConfiguration(), outPath, ClusterCenter.class,
				IntWritable.class);
		final IntWritable value = new IntWritable(0);
		for (ClusterCenter center : centers) {
			out.append(center, value);
		}
		System.out.println(centers.size());
		out.close();
	}
}
