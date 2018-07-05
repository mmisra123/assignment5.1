package ass;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SongMapper3 extends Mapper<LongWritable,Text,Text,IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private static final IntWritable zero = new IntWritable(0);
	private Text word  = new Text();
	
	//1st Column – User ID
	//2nd Column – TrackId
	//3rd Column – Songs share status (1 for shared, 0 for not)
	//4th Column – Listening platform (0 for Radio, 1 for Web)
	//5th Column – Song listening status (0 for skipped, 1 for fully heard)
 	//What are the number of times a song was shared

	
	@Override 
	public void map(LongWritable key, Text val,Context context ) throws IOException, InterruptedException
	{
		String value = val.toString();
		String result[] = value.split("\\|");
		
		// result[2] = shared status
		// result[1] = track ID
		
		if(result[2].equals("1"))
		{
			// if a song was shared then write 1 against its track ID
			context.write(new Text(result[1]),one);
			
		}
		else
		{
			context.write(new Text(result[1]),zero);
		}
	}

}



