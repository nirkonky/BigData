package Vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import Vector.Vector;
//extends Vector
public class ClusterCenter  implements WritableComparable<ClusterCenter> {
	private Vector center;
	private ClusterCenter[] centroids;




	public ClusterCenter() {
		super();
		//this.center = null;
	}

	public ClusterCenter(ClusterCenter other) {
		super();
		//System.out.println("in Copy Constructor!");
		this.center = new Vector(other.center);
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

	public boolean isVectorInRadius(Vector vector){
		double distance = DistanceMeasurer.measureDistance(this,vector);
		System.out.println(distance);
		if ( distance <= DistanceMeasurer.T1 && distance > DistanceMeasurer.T2) //almost the same
			return true;
		return false;
		
	}
	
	public ClusterCenter(Vector other) {
		super();
		this.center = other;
	}

	public boolean converged(ClusterCenter c) {
								  //true : false
		//System.out.println("in: converged");
		return compareTo(c) == 0 ? false : true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//System.out.println("in: write");
		center.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		center = new Vector();		
		this.center.readFields(in);
	}

	@Override
	public int compareTo(ClusterCenter o) {
		return center.compareTo(o.getCenter());
	}

	public ClusterCenter[] getCentroids() {
		return centroids;
	}

	public void setCentroids(ClusterCenter[] centroids) {
		this.centroids = centroids;
	}
	
	/**
	 * @return the center
	 */
	public Vector getCenter() {
		return center;
	}

	@Override
	public String toString() {
		return "ClusterCenter [center=" + center + "]";
	}




}
