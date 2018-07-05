package ass;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SongMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			_main(args);
		}
		catch(Exception e)
		{
			System.out.println("Exception while running the job");
			e.printStackTrace();
		}
	}
	public static void _main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		
		System.out.println("Song database Example");
		
		if(args.length != 3)
		{
			System.out.println("Valid options are:");
			System.out.println("SongExample task1 <input path> <output path>");
			System.out.println("SongExample task2 <input path> <output path>");
			System.out.println("SongExample task3 <input path> <output path>");
			System.exit(-1);
		}
		
		SongMain tv = new SongMain();
		tv.tvRun(args);
		
		System.out.println("Song Example Success");
	

}
	public SongMain()
	{
	    // constructor, nothing to be done here
		
	}
	private void tvRun(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		String option = args[0];
		Path input = new Path(args[1]);
		Path output = new Path(args[2]);
		// validate input output paths
	
		
		try
		{
			// this case to find number of unique Listeners in the dataset
			if(option.equalsIgnoreCase("task1"))
			{
				// init common stuff
				Job j = setupJob();
				// input and output path for the job
				FileInputFormat.setInputPaths(j, input);
				FileOutputFormat.setOutputPath(j, output);
				// set mapper class for this purpose
				j.setMapperClass(ass.SongMapper1.class);
				j.setReducerClass(ass.SongReducer.class);
				j.setNumReduceTasks(1);
				executeJob(j);
			}
			//What are the number of times a song was heard fully
			else if(option.equalsIgnoreCase("task2"))
			{
				// init common stuff
				Job j = setupJob();
				// input and output path for the job
				FileInputFormat.setInputPaths(j, input);
				FileOutputFormat.setOutputPath(j,output);
				j.setMapperClass(ass.SongMapper2.class);
				j.setReducerClass(ass.SongReducer.class);
				j.setNumReduceTasks(1);
				executeJob(j);

			}
			//What are the number of times a song was shared 
			else if(option.equalsIgnoreCase("task3"))
			{
				// init common stuff
				Job j = setupJob();
				// input and output path for the job
				FileInputFormat.setInputPaths(j, input);
				FileOutputFormat.setOutputPath(j,output);
				j.setMapperClass(ass.SongMapper3.class);
				j.setReducerClass(ass.SongReducer.class);
				j.setNumReduceTasks(1);
				executeJob(j);
				
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();

		}
		
		
	}
	private void executeJob(Job job) throws ClassNotFoundException, IOException, InterruptedException
	{
		if(job.waitForCompletion(true))
		{
			System.out.println("Job success");
		}
		else
		{
			System.out.println("Job Failed");
		}	
	}
	private Job setupJob() throws IOException
	{
		Job job = Job.getInstance();
		job.setJarByClass(SongMain.class);
		job.setJobName("SongExample");
		// input/output file formats for the job
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		// input/output file formats for the job
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		// type of K,V output of the job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		return job;
		
	}
	

}



	
	
	
