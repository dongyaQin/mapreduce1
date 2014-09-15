package inverted_index;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;


public class InvertedIndex {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		try {
			PropertyConfigurator.configure("log4j.properties");
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "daopai");
			job.setJarByClass(InvertedIndex.class);
			job.setMapperClass(InvertedIndexMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setCombinerClass(InvertedIndexCombiner.class);
			job.setReducerClass(InvertedIndexReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.101:9000/test/input"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.101:9000/test/output4"));
			System.out.println("¿ªÊ¼");
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			System.out.println("½áÊø");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
