package ac.konky.nir.vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;
//for commit

public class Vector implements WritableComparable<Vector> {

	private double[] vectorArr;
	private int neighbors;
	private String name;
	
	public Vector() {
		super();
	}

	public Vector(Vector v) {
		super();
		this.setNeighbors(v.getNeighbors());
		int l = v.vectorArr.length;
		this.vectorArr = new double[l];
		this.name = v.name;
		System.arraycopy(v.vectorArr, 0, this.vectorArr, 0, l);
	}

	public Vector(double[] a) {
		super();
		this.vectorArr = a;
	}
	
	public Vector(String name, double[] arr)
	{
		this.vectorArr = arr;
		this.name = name;
	}
	
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(vectorArr.length);
		for (int i = 0; i < vectorArr.length; i++)
			out.writeDouble(vectorArr[i]);
		out.writeInt(neighbors);
		out.writeChars(name);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		vectorArr = new double[size];
		for (int i = 0; i < size; i++)
			vectorArr[i] = in.readDouble();
		neighbors=in.readInt();
		this.name = in.readUTF();
	}

	@Override
	public int compareTo(Vector o) {

		@SuppressWarnings("unused")
		boolean equals = true;
		for (int i = 0; i < vectorArr.length; i++) {
			double c = vectorArr[i] - o.vectorArr[i];
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

	public double[] getVectorArr() {
		return vectorArr;
	}

	public void setVectorArr(double[] vector) {
		this.vectorArr = vector;
	}

	

	@Override
	public String toString() {
		return "Vector [vectorArr=" + Arrays.toString(vectorArr) + ", name="+ name + "]";
	}

	public int getNeighbors() {
		return neighbors;
	}

	public void setNeighbors(int neighbors) {
		this.neighbors = neighbors;
	}

}
