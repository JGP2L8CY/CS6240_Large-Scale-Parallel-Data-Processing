package twitter;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterFollowerCount extends Configured implements Tool {

	public static class NodeMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable zero = new IntWritable(0);
		private Text nodeId = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			nodeId.set(value);
			context.write(nodeId, zero);
		}
	}

	public static class EdgeMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text followerId = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] userIds = value.toString().split(",");
			if (userIds.length == 2) {
				followerId.set(userIds[1]);
				context.write(followerId, one);
			}
		}
	}

	public static class JoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int followerCount = 0;
			boolean isNodePresent = false;

			for (IntWritable val : values) {
				if (val.get() == 0) { // 노드가 존재함
					isNodePresent = true;
				} else { // 엣지에 대한 팔로워 수
					followerCount += val.get();
				}
			}

			if (isNodePresent) {
				result.set(followerCount);
				context.write(key, result);
			}
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: TwitterFollowerCount <edges input path> <nodes input path> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf(), "Twitter Follower Count");
		job.setJarByClass(TwitterFollowerCount.class);

		// MultipleInputs를 사용하여 각 파일에 맞는 Mapper 클래스를 지정
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EdgeMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, NodeMapper.class);

		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static void main(final String[] args) {
		try {
			int res = ToolRunner.run(new TwitterFollowerCount(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
