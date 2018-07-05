package ass;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SongMapper1 extends Mapper<LongWritable,Text,Text,IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	
	
	//1st Column – User ID
	//2nd Column – TrackId
	//3rd Column – Songs share status (1 for shared, 0 for not)
	//4th Column – Listening platform (0 for Radio, 1 for Web)
	//5th Column – Song listening status (0 for skipped, 1 for fully heard)

	// find out number of unique users
	
	@Override 
	public void map(LongWritable key, Text val,Context context ) throws IOException, InterruptedException
	{
		String value = val.toString();
		String result[] = value.split("\\|");
		
		// first column is the user name. Output the user name with count of 1
		// These will be aggregated by reducer
		context.write(new Text(result[0]),one);
		
	}

}



