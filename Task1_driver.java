import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1_driver extends Configured{

	// Driver function to execute the Map reduce job
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//Creating Configuration object and Job Object
		Configuration conf = new Configuration();
		Job job =Job.getInstance(conf, "Invalid Data Removal using Map-only Task");
		
		// Setting Main class, Map and Reduce class 
		job.setJarByClass(Task1_driver.class);
		job.setMapperClass(Task1_mapper.class);
		
		// Setting Number of reducer Tasks to 0 as no Reducer will be used
		job.setNumReduceTasks(0);
		
		// Setting Map Output Key, value type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Setting Reducer Output Key, value type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Setting Input and Output Directory
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//System.exit() will return 0 on successful execution, else -1
		System.exit(job.waitForCompletion(true) ? 0 : -1);
		
	}

}
