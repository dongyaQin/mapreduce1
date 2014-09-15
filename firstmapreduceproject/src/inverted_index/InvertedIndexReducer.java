package inverted_index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		try {
			String s = "@";
//			System.out.println("----------------------------------");
			 for (Text val : values) {
				 System.out.println(val.toString());
			      s += val.toString();
			 }
			 s+="@";
//			 System.out.println("----------------------------------");
			 System.out.println(key.toString()+" "+s);
			 context.write(key, new Text(s));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	
}
