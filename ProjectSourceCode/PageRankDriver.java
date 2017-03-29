
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class PageRankDriver extends Configured implements Tool {
  public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new PageRankDriver(),args);
    System.exit(exitCode);
  }

	@Override
	public int run(String[] args) throws Exception {
			if (args.length != 2) {
				  System.err.println("Usage: WordCount <input path> <output path>");
				  System.exit(-1);
				}

			//Initializing the map reduce job
			Job job= new Job(getConf());
			job.setJarByClass(PageRankDriver.class);
			job.setJobName("pagerank");
			

			//Setting the input and output paths.The output file should not already exist. 
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//Setting the mapper, reducer, and combiner classes
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			//job.setCombinerClass(PageRankReducer.class);

			job.setInputFormatClass(DBLPInputFormat.class);
			//Setting the format of the key-value pair to write in the output file.
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			
			//Submit the job and wait for its completion
			return(job.waitForCompletion(true) ? 0 : 1);
	}
}
