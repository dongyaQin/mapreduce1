package inverted_index;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
	FileSplit split;
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			System.out.println("maper excute1111");
			split = (FileSplit)context.getInputSplit();
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text k = new Text();
			Text v = new Text(split.getPath().toString());
		    while (itr.hasMoreTokens()) {
		      k.set(itr.nextToken());
		      context.write(k, v);
		    }
		    System.out.println(k.toString()+"-->"+v.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
