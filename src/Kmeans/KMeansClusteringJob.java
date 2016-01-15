package Kmeans;

import java.beans.XMLDecoder;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import ac.lemberg.kobi.properties.HadoopProperties;


import Vector.ClusterCenter;
import Vector.DistanceMeasurer;
import Vector.Vector;

public class KMeansClusteringJob {
	
	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		//Read properties from windows
		HadoopProperties properties = new HadoopProperties();
		try 
		{
			XMLDecoder decoder=new XMLDecoder(new BufferedInputStream(new FileInputStream("/home/training/HadoopProperties.xml")));
			properties = ((HadoopProperties)decoder.readObject());
			decoder.close();			
		} catch (FileNotFoundException e) 
		{
			System.out.println("ERROR: File Settings/HadoopProperties.xml not found");
		}
		
		//setting the input  and data from windows 
		Path data = new Path(properties.getJobServerInputFolderPath()+"/vectors.csv");
		
		//setting the output from canopy algorithm
		Path canopyOutPut = new Path(properties.getJobServerInputFolderPath()+"/canopyOutput");
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		//Removing output file if it alrdy exists
		if (fs.exists(canopyOutPut))
			fs.delete(canopyOutPut, true);
		
		//initlize vector size path
		Path vectorSizePath = new Path("/home/training/vectorSize.seq");
		conf.set("vectorSize.path", vectorSizePath.toString());
		
		//writh him to the sequence file
		final SequenceFile.Writer vectorSizeWriter = SequenceFile.createWriter(fs, conf,vectorSizePath,IntWritable.class,IntWritable.class);		
		vectorSizeWriter.append(new IntWritable(properties.getNumOfFeatures()),new IntWritable(1));
		vectorSizeWriter.close();
				
		
		//Running canopy Clustering Job
		Job canopyJob = new Job(conf);
		    
		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		 canopyJob.setJarByClass(CanopyMapper.class);
		    
		 /*
		  * Specify an easily-decipherable name for the job.
		  * This job name will appear in reports and logs.
		  */
		 canopyJob.setJobName("Canopy Clustering");

		 /*
		  * Specify the paths to the input and output data based on the
		  * command-line arguments.
		  */
		 FileInputFormat.setInputPaths(canopyJob, data);
		 FileOutputFormat.setOutputPath(canopyJob,canopyOutPut );

		 /*
		  * Specify the mapper and reducer classes.
		  */
		 canopyJob.setMapperClass(CanopyMapper.class);
		 canopyJob.setReducerClass(CanopyReducer.class);
		 canopyJob.setMapOutputKeyClass(IntWritable.class);
		 canopyJob.setMapOutputValueClass(ClusterCenter.class);
		    
		 /*
		  * Specify the job's output key and value classes.
		  */
		 canopyJob.setOutputKeyClass(ClusterCenter.class);
		 canopyJob.setOutputValueClass(DoubleWritable.class);


		 /*
		  * Start the MapReduce job and wait for it to finish.
		  * If it finishes successfully, return 0. If not, return 1.
		  */
		 boolean success = canopyJob.waitForCompletion(true);
		 //System.exit(success ? 0 : 1);
		  
		
		
		/*
		 * Kmeans start after Canopy Finish!
		 * 
		 * 
		 * starting kmeans Job!
		 * 
		 */
		
		 //creating random object to calculate the K centroids
		Random rand = new Random();
		
		
		int k=properties.getNumOfClusters();
		int iteration = 1;
		conf.set("num.iteration", iteration + "");
		
		
		
		
		//output path for depth1
		Path out = new Path("files/clustering/depth_1");
		
		//Need to fix it getting the path of canopyCenters
		Path canopyPath = new Path("/home/training/Desktop/Kmeans/canopyCenters.seq");
		
		
		//todo:
		//Path canopy = new Path(conf.get("canopy.path"));
		
		//ArrayList<ClusterCenter> centers = new ArrayList<ClusterCenter>();
		ArrayList<ClusterCenter> arrayCenters = new ArrayList<ClusterCenter>();
		
		//hash map ClusterCenter to how much k centroid he will get.
		HashMap<ClusterCenter,Double> hashMapCanopyToK = new HashMap<ClusterCenter,Double>();
		
		//Reading the canopy Path with how much neighbors/All he have
		SequenceFile.Reader canopyCentersReader = new SequenceFile.Reader(fs, canopyPath, conf);
		ClusterCenter centerReader ;
		DoubleWritable neighbors ;
		while (canopyCentersReader.next(centerReader = new ClusterCenter() , neighbors = new DoubleWritable())) {	
			arrayCenters.add(centerReader);
			hashMapCanopyToK.put(centerReader, Math.ceil(new Double(neighbors+"")*k));
		}
		canopyCentersReader.close();

		//check if there is Canopy with k==1 if there is, take from someone that have 3 or  more.
		if (hashMapCanopyToK.containsValue(1.0)){
			for (Entry<ClusterCenter, Double> entrySet : hashMapCanopyToK.entrySet()) {
				Double value = entrySet.getValue();
				if (value==1||value==1.0){
					for (ClusterCenter canopyCenter : hashMapCanopyToK.keySet()){
						if (hashMapCanopyToK.get(canopyCenter)>=3 || hashMapCanopyToK.get(canopyCenter)>=3.0 ){
							hashMapCanopyToK.put(canopyCenter, hashMapCanopyToK.get(canopyCenter)-1);
							hashMapCanopyToK.put(entrySet.getKey(), entrySet.getValue()+1);
						}
					}
				}
			}
		}
		
		
		//Creating hash map from clusterCenter > hes k vectors.
		HashMap<ClusterCenter,Vector[]> CanopyCentroids = new HashMap<ClusterCenter,Vector[]>();
		
		//TupleWritable tw = new TupleWritable(new Tuple<>);
		
		for (Entry<ClusterCenter, Double> entrySet : hashMapCanopyToK.entrySet()) 
		{
			int centroidNumber = new Integer(entrySet.getValue().toString().substring(0,entrySet.getValue().toString().indexOf("."))).intValue();
			System.out.println(centroidNumber);
			Vector[] vectorOfCentroids = new Vector[centroidNumber];
			for (int i = 0; i < centroidNumber; i++) {
				int vectorSize = entrySet.getKey().getCenter().getVector().length;
				double[] vectorConstructor = new double [vectorSize];
				for (int j = 0; j < vectorSize; j++) {
					double random = rand.nextDouble();
					double feature = (random)/(random+DistanceMeasurer.T1) + entrySet.getKey().getCenter().getVector()[j];
					vectorConstructor[j] = feature;
				}
		
				Vector v = new Vector(vectorConstructor);
				vectorOfCentroids[i] = v;
			}
			CanopyCentroids.put(entrySet.getKey(),vectorOfCentroids);
			
			
		}
		
		//Optiynal
		//Creating cetnroid path
		Path center = new Path("/home/training/Desktop/Kmeans/centroid/centroid.seq");
		conf.set("centroid.path", center.toString());
		
		
		//Writer the K centroid (for each key(centroid), value(1)
		final SequenceFile.Writer centroidWriter = SequenceFile.createWriter(fs,
				conf, center, ClusterCenter.class, IntWritable.class);

		for (Entry<ClusterCenter, Vector[]> can : CanopyCentroids.entrySet()) {
			for (Vector v :  can.getValue()) {
				centroidWriter.append(new ClusterCenter(v), new IntWritable(1));
			}
		}
		centroidWriter.close();

		//writer each canopy with list of hes centroids
		
		
		Path canopyWithK = new Path("/home/training/canopyWithK.seq");
		conf.set("canopyWithK.path", canopyWithK.toString());

		
		final SequenceFile.Writer canopyWithKWriter = SequenceFile.createWriter(fs, conf,canopyWithK,ClusterCenter.class,ArrayWritable.class);		
		for (ClusterCenter cs: CanopyCentroids.keySet()) 
		{
			canopyWithKWriter.append(cs, new ArrayWritable(Vector.class,CanopyCentroids.get(cs)));
		}
		canopyWithKWriter.close();
		
		
		
		Job job = new Job(conf);
		job.setJobName("KMeans Clustering");

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);

		if (fs.exists(out))
			fs.delete(out, true);

		if (fs.exists(center))
			fs.delete(out, true);

	    FileInputFormat.setInputPaths(job, new Path(args[0]).toString());

		SequenceFileOutputFormat.setOutputPath(job, out);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(ClusterCenter.class);
		job.setOutputValueClass(Vector.class);

		job.waitForCompletion(true);

		long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		iteration++;
		while (counter > 0) {
			conf = new Configuration();
			conf.set("centroid.path", center.toString());
			conf.set("num.iteration", iteration + "");
			job = new Job(conf);
			job.setJobName("KMeans Clustering " + iteration);

			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);
			Path in =  new Path("/home/training/vectors.csv");
			System.out.println("files/clustering/depth_" + iteration);
			out = new Path("files/clustering/depth_" + iteration);

			SequenceFileInputFormat.addInputPath(job, in);
			if (fs.exists(out))
				fs.delete(out, true);

			SequenceFileOutputFormat.setOutputPath(job, out);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(ClusterCenter.class);
			job.setOutputValueClass(Vector.class);
			job.waitForCompletion(true);
			iteration++;
			counter = job.getCounters()
					.findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}
		
		//Path result = new Path(outPutDic + (iteration - 1) + "/");
		System.out.println("files/clustering/depth_" + (iteration - 1) + "/");
		Path result = new Path("files/clustering/depth_" + (iteration - 1) + "/part-r-00000");
		//FileStatus[] stati = fs.listStatus(result);
		//for (FileStatus status : stati) {
		//	if (!status.isDir()) {
				//Path path = status.getPath();
		//		Path path = new Path("hdfs://0.0.0.0/user/cloudera/files/clustering/depth_3/part-r-00000");
				/*SequenceFile.Reader reader = new SequenceFile.Reader(fs, result, conf);
				ClusterCenter key = new ClusterCenter();
				Vector v = new Vector();
				int i=0;
				ClusterCenter temp = new ClusterCenter();
				while (reader.next(key, v)) {
					if (key!=temp){
						i++;
						temp = key;
					}
					//System.out.println("K-->"+ key + "   VEctor   ->> "+v);
				}
				System.out.println(i);
				reader.close();
	*/
			}
}
