import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1_mapper extends Mapper<Object, Text,Text,Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String temp_value = value.toString();
		
		// Do operation only if read line does not contains 
		// invalid record denoted by NA. There can be three 
		// variations : NA| , |NA| and |NA
		// Do not write the invalid records to output file.
		if (!(temp_value.contains("NA|")  || 
			  temp_value.contains("|NA|") ||
			  temp_value.contains("|NA")))
		{
			context.write(value, null);
		}
		
	}
}
