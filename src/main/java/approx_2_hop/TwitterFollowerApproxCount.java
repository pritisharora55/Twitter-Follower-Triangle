package approx_2_hop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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


public class TwitterFollowerApproxCount extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(TwitterFollowerApproxCount.class);

	enum GlobalCounter {
        COUNT
    }

	public static int MAX = 100000;

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final Text user = new Text();
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String[] user_ids = itr.nextToken().split(",");

				if ((Integer.parseInt(user_ids[0]) < MAX) && (Integer.parseInt(user_ids[1]) < MAX)){

                    user.set(user_ids[0]);
                    context.write(user,new Text( user_ids[0]+","+user_ids[1]+","+"O"));

                    user.set(user_ids[1]);
                    context.write(user, new Text( user_ids[0]+","+user_ids[1]+","+"I"));
			}
		}
	}
}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		//private final LongWritable result = new LongWritable();
		

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			
			List incoming = new ArrayList<>();
            List outgoing = new ArrayList<>();

            // summing up all followers for this key
            for (final Text val : values) {
                String[] value_list = val.toString().split(",");
                if (value_list[2].equals("I")){
                    incoming.add(value_list[0]);
                } else {
                    outgoing.add(value_list[1]);
                }

            }

            for ( int i = 0; i<incoming.size(); i++){
                for (int j = 0; j<outgoing.size(); j++){
                    context.write(new Text("2HopPaths"), new Text( incoming.get(i)+ "," + key.toString() + "," + outgoing.get(j)));
                }
            }
            context.getCounter(GlobalCounter.COUNT).increment(incoming.size()*outgoing.size());

		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Follower Count");
		job.setJarByClass(TwitterFollowerApproxCount.class);
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
			ToolRunner.run(new TwitterFollowerApproxCount(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}