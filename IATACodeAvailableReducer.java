
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IATACodeAvailableReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	int count = 0;
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable count : values) {
			sum += count.get();
		}
		count++;
		//context.write(key, new IntWritable(sum));
	}
	public void cleanup(Context context) throws InterruptedException, IOException {
		context.write(new Text("IATA code avilavle for airlines"), new IntWritable(count));
	}
}
