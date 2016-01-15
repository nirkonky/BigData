package Vector;

import Vector.Vector;

public class DistanceMeasurer {

	public static final double T1 = 4125;
	public static final double T2 = 0;


	public static final double measureDistance(ClusterCenter center, Vector v) {
		double sum = 0;
		int length = v.getVector().length;
		for (int i = 0; i < length; i++) {
			sum += Math.abs(center.getCenter().getVector()[i]
					- v.getVector()[i]);
		}

		return sum;
	}

}
