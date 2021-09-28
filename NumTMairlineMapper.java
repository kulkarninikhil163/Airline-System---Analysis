
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class NumTMairlineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text airlines = new Text();
	Text functional = new Text();
	IntWritable count = new IntWritable();
	private static final Logger LOG = Logger.getLogger(NumTMairlineMapper.class);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] list = value.toString().split(",");

		if (key.get() != 0) {
			airlines.set(list[1]);
			functional.set(list[7]);
			String s=""+functional;
			System.out.println(s);
			if(s.equals("Y"))
			{
				System.out.println("fff");
				count.set(1);
			}
			else
				count.set(0);
			//count.set(Integer.parseInt(list[0]));
			LOG.info("Flight analysis");
			context.write(airlines, count);
		}
		LOG.info("Skipped line with key:" + key.get());
	}

}
