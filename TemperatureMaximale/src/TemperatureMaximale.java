import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class TemperatureMaximale {
	
	public static class TemperatureMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private Text ville = new Text();
		private IntWritable temperature = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();
				String villeStr = line.split(",")[0];
				int temperatureInt = Integer.parseInt(line.split(",")[1]);
				ville.set(villeStr);
		        temperature.set(temperatureInt);
		        context.write(ville, temperature);
			}			
		}
		
	}
	
	public static class TemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE; 
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			result.set(maxValue);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "temperature");
	    job.setJarByClass(TemperatureMaximale.class);
	    job.setMapperClass(TemperatureMapper.class);
	    job.setCombinerClass(TemperatureReducer.class);
	    job.setReducerClass(TemperatureReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
