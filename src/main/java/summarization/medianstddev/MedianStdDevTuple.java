package summarization.medianstddev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MedianStdDevTuple implements Writable {
	private float median = 0;
	private float stdDev = 0;

	public float getMedian() {
		return median;
	}

	public void setMedian(float median) {
		this.median = median;
	}

	public float getStdDev() {
		return stdDev;
	}

	public void setStdDev(float stdDev) {
		this.stdDev = stdDev;
	}

	public void readFields(DataInput in) throws IOException {
		// Read the data out in the order it is written,
		// creating new Date objects from the UNIX timestamp
		median = in.readLong();
		stdDev = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		// Write the data out in the order it is read,
		// using the UNIX timestamp to represent the Date
		out.writeFloat(median);
		out.writeFloat(stdDev);
	}

	public String toString() {
		return median + "\t" + stdDev;
	}
}
