
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class IATACodeAvailableMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text airlines = new Text();
	Text IATA = new Text();
	IntWritable count = new IntWritable();
	private static final Logger LOG = Logger.getLogger(IATACodeAvailableMapper.class);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] list = value.toString().split(",");

		if (key.get() != 0) {
			airlines.set(list[1]);
			IATA.set(list[3]);
			String s=""+IATA;
			System.out.println(s);
			if(s.equals(""))
			{				System.out.println("fff");
				count.set(0);
			}
			else
				count.set(1);
			//count.set(Integer.parseInt(list[0]));
			LOG.info("Flight analysis");
			context.write(airlines, count);
		}
		LOG.info("Skipped line with key:" + key.get());
	}

}
