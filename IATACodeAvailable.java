
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IATACodeAvailable extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job stateWiseCount = Job.getInstance(getConf());
		stateWiseCount.setJobName("Airlines Data Analysis");
		stateWiseCount.setJarByClass(IATACodeAvailable.class);

		/* Field separator for reducer output*/
		stateWiseCount.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
		stateWiseCount.setMapperClass(IATACodeAvailableMapper.class);
		stateWiseCount.setReducerClass(IATACodeAvailableReducer.class);

		stateWiseCount.setInputFormatClass(TextInputFormat.class);
		stateWiseCount.setMapOutputKeyClass(Text.class);
		stateWiseCount.setMapOutputValueClass(IntWritable.class);

		stateWiseCount.setOutputKeyClass(Text.class);
		stateWiseCount.setOutputValueClass(IntWritable.class);

		Path inputFilePath = new Path("airlines.csv");
		Path outputFilePath = new Path("output333s");

		FileInputFormat.addInputPath(stateWiseCount, inputFilePath);
		FileOutputFormat.setOutputPath(stateWiseCount, outputFilePath);
		
				
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		return(stateWiseCount.waitForCompletion(true) ? 0 : 1);
		
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new IATACodeAvailable(), args);
	}

}
