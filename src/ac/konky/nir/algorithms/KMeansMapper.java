package ac.konky.nir.algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import ac.konky.nir.vector.CenterCentroidWritable;
import ac.konky.nir.vector.ClusterCenter;
import ac.konky.nir.vector.DistanceMeasurer;
import ac.konky.nir.vector.Vector;

//for commit

public class KMeansMapper extends
		Mapper<LongWritable,Text, ClusterCenter, Vector> {

	List<ClusterCenter> centers = new LinkedList<ClusterCenter>();
	List<ClusterCenter> canopys;
	HashMap<ClusterCenter,Vector[]> CanopyCentroidsWithK;
	int vectorSize;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		

		
		
		
		
		Configuration conf = context.getConfiguration();
		this.vectorSize = new Integer(conf.get("vectorSize")).intValue();
		FileSystem fs = FileSystem.get(conf);

		//Path vectorSizePath = new Path("/home/training/vectorSize.seq");
		/*SequenceFile.Reader canopyVectorSizeReader = new SequenceFile.Reader(fs, vectorSizePath,conf);
		IntWritable vectorSizeKey = new IntWritable();
		IntWritable vectorSizeValue = new IntWritable();
		while (canopyVectorSizeReader.next(vectorSizeKey, vectorSizeValue)) {
			vectorSize = new Integer(vectorSizeKey+"");
		}
		canopyVectorSizeReader.close();*/
		
		
		
		
		
		Path canopyCenters = new Path("files/CanopyClusterCenters/canopyCenters.seq");
		
		//Reading canopy.
		canopys = new LinkedList<ClusterCenter>();
		@SuppressWarnings("deprecation")
		SequenceFile.Reader canopyReader = new SequenceFile.Reader(fs, canopyCenters,conf);
		ClusterCenter canopyKey = new ClusterCenter();
		DoubleWritable canopyVal = new DoubleWritable();
		while (canopyReader.next(canopyKey, canopyVal)) 
		{
			canopys.add(canopyKey);
		}
		canopyReader.close();
		
		
		
		
		Path canopyWithCentroids = new Path("files/KmeansCentroids/canopyWithCentroids.seq");
		SequenceFile.Reader canopyWithKReader = new SequenceFile.Reader(fs, canopyWithCentroids,
				conf);
		CanopyCentroidsWithK = new HashMap<ClusterCenter,Vector[]>();
		
		ClusterCenter center = new ClusterCenter();
		ArrayWritable vectorToCanopy = new ArrayWritable(Vector.class) ;
		while (canopyWithKReader.next(center, vectorToCanopy)) {
			CanopyCentroidsWithK.put(center, (Vector[]) vectorToCanopy.toArray());
			
		}
		canopyReader.close();
		
		
		//Syso
		
		//Reading centroids
		Path centroids = new Path("files/KmeansCentroids/centroids.seq");
		@SuppressWarnings("deprecation")
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids,
				conf);
		ClusterCenter key = new ClusterCenter();
		IntWritable value = new IntWritable();
		while (reader.next(key, value)) {
			centers.add(new ClusterCenter(key));
		}
		reader.close();
	}

	@Override
	protected void map(LongWritable key,Text value, Context context)
			throws IOException, InterruptedException {
		ArrayList<String> Data = new ArrayList<String>(Arrays.asList(value.toString().split(",")));
		for (int i = 1; i < Data.size()-vectorSize; i+=vectorSize) 
		{
			double[] doubleArray = new double[vectorSize];
			for (int j = 0; j < vectorSize; j++) {
				doubleArray[j] = new Double(Data.get(i+j).substring(1, Data.get(i+j).length()-1));
			}
			Vector newVector = new Vector(doubleArray);
			
			
			ClusterCenter nearestCenter = canopys.get(0);
			double nearestDistanceCanopy = Double.MAX_VALUE;
			//Double dist = null;
			Double distFromCanopy = null;
			for (ClusterCenter canopyCenter : canopys) {
				distFromCanopy = DistanceMeasurer.measureDistance(canopyCenter, newVector);
					if (distFromCanopy<nearestDistanceCanopy){
							nearestCenter = canopyCenter;
							nearestDistanceCanopy = distFromCanopy;
						}
					}
			Vector nearestK = CanopyCentroidsWithK.get(nearestCenter)[0];
			double nearestDistanceK = Double.MAX_VALUE;
			//if p and k share the same canopy center
			for (Vector vectorK : CanopyCentroidsWithK.get(nearestCenter))
			{
				distFromCanopy = DistanceMeasurer.measureDistance(new ClusterCenter(vectorK), newVector);
				if (nearestDistanceK>distFromCanopy){
						nearestK = vectorK;
						nearestDistanceK = distFromCanopy;
					}
			}
			//System.out.println("nearestK-->"+ nearestK+" , vector-->"+ newVector);
			//CenterCentroidWritable ccw = new CenterCentroidWritable(nearestCenter, nearestK);
			context.write(new ClusterCenter(nearestK), newVector);

		}
	}
}
