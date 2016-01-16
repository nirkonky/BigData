package ac.konky.nir.vector;
//for commit

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import ac.konky.nir.vector.Vector;

//extends Vector
public class CenterCentroidWritable  implements WritableComparable<CenterCentroidWritable> {
	private ClusterCenter center;
	private Vector centroid;




	public CenterCentroidWritable() {
		super();
		//this.center = null;
	}
	

	public CenterCentroidWritable(ClusterCenter center, Vector centroid) {
		super();
		this.center = center;
		this.centroid = centroid;
	}

	public CenterCentroidWritable(CenterCentroidWritable other) {
		super();
		//System.out.println("in Copy Constructor!");
		this.center = new ClusterCenter(other.center);
		this.centroid = new Vector(other.centroid);
		//this.setNeighbors(center.getNeighbors());
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return this.toString().equals(obj.toString());
	}


	@Override
	public void write(DataOutput out) throws IOException {
		//System.out.println("in: write");
		center.write(out);
		centroid.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		center = new ClusterCenter();
		centroid = new Vector();
		this.center.readFields(in);
		this.centroid.readFields(in);
	}

	@Override
	public int compareTo(CenterCentroidWritable o) {
		if(center.compareTo(o.getCenter())==0)
				return centroid.compareTo(o.getCentroid());
		return center.compareTo(o.getCenter());
	}

	@Override
	public String toString() {
		return "CenterCentroidWritable [center=" + center + ", centroid="
				+ centroid + "]";
	}
	

	public ClusterCenter getCenter() {
		return center;
	}

	public void setCenter(ClusterCenter center) {
		this.center = center;
	}

	public Vector getCentroid() {
		return centroid;
	}

	public void setCentroid(Vector centroid) {
		this.centroid = centroid;
	}





}
