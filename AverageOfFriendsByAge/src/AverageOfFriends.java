import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageOfFriends {
	
	public static class FriendsMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		
		private IntWritable age = new IntWritable();
		private IntWritable friends = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String line = itr.nextToken();
				int ageInt = Integer.parseInt(line.split(",")[0]);
				int friendsInt = Integer.parseInt(line.split(",")[1]);
				age.set(ageInt);
		        friends.set(friendsInt);
		        context.write(age, friends);
			}			
		}
		
	}
	
	public static class FriendsReducer extends Reducer<IntWritable,IntWritable,IntWritable,FloatWritable> {
		private FloatWritable result = new FloatWritable();
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int totalFriends = 0;
			int totalUsers = 0;
			for (IntWritable value : values) {
				totalFriends += value.get();
				totalUsers++;
			}
			float avg = (float) totalFriends/totalUsers;
			result.set(avg);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "friends");
	    job.setJarByClass(AverageOfFriends.class);
	    job.setMapperClass(FriendsMapper.class);
	    job.setReducerClass(FriendsReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
