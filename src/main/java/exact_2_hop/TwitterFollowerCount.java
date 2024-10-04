package exact_2_hop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class TwitterFollowerCount extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(TwitterFollowerCount.class);

	enum GlobalCounter {
        COUNT
    }

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final Text user = new Text();
		private final Text edge_type = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String[] user_ids = itr.nextToken().split(",");

				user.set(user_ids[0]);
                edge_type.set("O");
                context.write(user, edge_type);

                user.set(user_ids[1]);
                edge_type.set("I");
                context.write(user, edge_type);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		//private final LongWritable result = new LongWritable();
		

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			long total = 0;
			long m = 0;
	        long n = 0;

            
			for (final Text val : values) {
				if (val.toString().equals("O")) {
					n += 1;
				} else if (val.toString().equals("I")) {
					m += 1;
				}
            }
			total = m * n;
			context.getCounter(GlobalCounter.COUNT).increment(total);
			
			//context.write(key, new Text(Long.toString(total)));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Follower Count");
		job.setJarByClass(TwitterFollowerCount.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int jobStatus = job.waitForCompletion(true) ? 0 : 1;

		Counter counter = job.getCounters().findCounter(GlobalCounter.COUNT);

        System.out.println(counter.getDisplayName() + ":" +counter.getValue());

		return jobStatus;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new TwitterFollowerCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}