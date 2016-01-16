package ac.konky.nir.algorithms;
//for commit

import java.beans.XMLDecoder;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ac.lemberg.kobi.properties.HadoopProperties;

public class tests {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		//Read properties from windows

		Configuration jobConfigurations = new Configuration();
		FileSystem fs = FileSystem.get(jobConfigurations);
		
		//Removing output file if it alrdy exists
//		if (fs.exists(canopyOutputPath)){fs.delete(canopyOutputPath, true);}
		
		//initlize vector size path
		Path vectorSizePath = new Path("/home/training/vectorSize.seq");
		jobConfigurations.set("vectorSize.path", vectorSizePath.toString());
		jobConfigurations.set("vectorSize",new Integer(7).toString());
		
		String a = jobConfigurations.get("vectorSize");
		System.out.println(a);

	}

}
