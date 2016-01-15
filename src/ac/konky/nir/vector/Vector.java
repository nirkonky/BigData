package ac.konky.nir.vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class Vector implements WritableComparable<Vector> {

	private double[] vector;
	private int neighbors;
	public Vector() {
		super();
	}

	public Vector(Vector v) {
		super();
		this.setNeighbors(v.getNeighbors());
		int l = v.vector.length;
		this.vector = new double[l];
		System.arraycopy(v.vector, 0, this.vector, 0, l);
	}

	public Vector(double[] a) {
		super();
		this.vector = a;
	}
	
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(vector.length);
		for (int i = 0; i < vector.length; i++)
			out.writeDouble(vector[i]);
		out.writeInt(neighbors);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		vector = new double[size];
		for (int i = 0; i < size; i++)
			vector[i] = in.readDouble();
		neighbors=in.readInt();
	}

	@Override
	public int compareTo(Vector o) {

		@SuppressWarnings("unused")
		boolean equals = true;
		for (int i = 0; i < vector.length; i++) {
			double c = vector[i] - o.vector[i];
			if (c!= 0.0d)
			{
				return (int)c;
			}		
		}
		return 0;
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return this.toString().equals(obj.toString());
	}

	public double[] getVector() {
		return vector;
	}

	public void setVector(double[] vector) {
		this.vector = vector;
	}

	@Override
	public String toString() {
		return "Vector [vector=" + Arrays.toString(vector) + "]";
	}

	public int getNeighbors() {
		return neighbors;
	}

	public void setNeighbors(int neighbors) {
		this.neighbors = neighbors;
	}

}
