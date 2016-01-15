package logsFilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class logWritable implements WritableComparable<logWritable> {

	
	private Text rowID,timestampOne,request,userName;
	

	
	public logWritable() {
		this.rowID = new Text();
		this.request = new Text();
		this.timestampOne = new Text();
		this.userName = new Text();
		
	}

	
	@Override
	public void readFields(DataInput in) throws IOException {
		rowID.readFields(in);
		request.readFields(in);
		timestampOne.readFields(in);
		userName.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		rowID.write(out);
		request.write(out);
		timestampOne.write(out);
		userName.write(out);

	}

	@Override
	public int compareTo(logWritable o) {
		if (userName.compareTo(o.userName)==0)
		{
			return(request.compareTo(o.request));
		}
		else
			return userName.compareTo(o.userName);
	}
	
	public boolean equals(Object o)
	{
		if (o instanceof logWritable)
		{
			logWritable other = (logWritable)o;
			return userName.equals(other.userName)&&request.equals(other.request);
		}
		return false;
	}
	
	public int hashCode(){
		return userName.hashCode();
	}

	public Text getRequest() {
		return request;
	}

	public void setRequest(Text request) {
		this.request = request;
	}

	public Text getTimestamp() {
		return timestampOne;
	}

	public void setTimestamp(Text timestamp) {
		this.timestampOne = timestamp;
	}

	public Text getUserName() {
		return userName;
	}

	public void setUserName(Text userName) {
		this.userName = userName;
	}

	public Text getRowID() {
		return rowID;
	}

	public void setRowID(Text rowID) {
		this.rowID = rowID;
	}

	@Override
	public String toString() {
		return "request= "+ request + " , userName=" + userName ;
				
	}


	
	

}
