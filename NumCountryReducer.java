
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumCountryReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	int max=0;
	int top2=0;
	int top3=0;
	int top4=0;
	int top5=0;
	String maxWord = "";
	String top2Word = "";
	String top3Word = "";
	String top4Word = "";
	String top5Word = "";
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable count : values) {
			sum += count.get();
		}
		
		if (max < sum) {
			top5 = top4;
			top4 = top3;
			top3 = top2;
			top2 = max;
			
			top5Word=top4Word;
			top4Word=top3Word;
			top3Word=top2Word;
			top2Word=maxWord;
			
			max = sum;
			maxWord = key.toString();
		}
		else if(top2<sum){
			top5 = top4;
			top4 = top3;
			top3 = top2;
			
			top5Word=top4Word;
			top4Word=top3Word;
			top3Word=top2Word;
			
			top2 = sum;
			top2Word = key.toString();
		}
		else if(top3<sum){
			top5 = top4;
			top4 = top3;
			
			top5Word=top4Word;
			top4Word=top3Word;
			
			top3 = sum;
			top3Word = key.toString();
		}
		else if(top4<sum){
			top5 = top4;
			
			top5Word=top4Word;
			
			top4 = sum;
			top4Word = key.toString();
		}
		else if(top5<sum){
			top5 = sum;
			top5Word = key.toString();
		}
		// context.write(key, new IntWritable(sum));
	}
	
	public void cleanup(Context context) throws InterruptedException, IOException {
		context.write(new Text(maxWord), new IntWritable(max));
		context.write(new Text(top2Word), new IntWritable(top2));
		context.write(new Text(top3Word), new IntWritable(top3));
		context.write(new Text(top4Word), new IntWritable(top4));
		context.write(new Text(top5Word), new IntWritable(top5));
	}
}
