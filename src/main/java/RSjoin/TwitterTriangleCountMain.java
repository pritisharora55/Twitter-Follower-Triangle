package RSjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TwitterTriangleCountMain extends Configured implements Tool {

    private static final Logger log = LogManager.getLogger(TwitterTriangleCountMain.class);
    public static int MAX_USERS = 30000;

    /**
     * Global counter for counting triangles
     */
    public enum COUNTERS {
        TRIANGLE_COUNT
    }

    public static class FollowerMapper extends Mapper<Object, Text, Text, Text> {

        private final Text userId = new Text();

        @Override
        public void map(final Object inputKey, final Text inputValue, final Context context) throws IOException, InterruptedException {
            final StringTokenizer tokens = new StringTokenizer(inputValue.toString());

            while (tokens.hasMoreTokens()) {
                String[] userPair = tokens.nextToken().split(",");

                if ((Integer.parseInt(userPair[0]) < MAX_USERS) && (Integer.parseInt(userPair[1]) < MAX_USERS)) {
                    userId.set(userPair[0]);
                    context.write(userId, new Text(userPair[0] + "," + userPair[1] + ",OUT"));

                    userId.set(userPair[1]);
                    context.write(userId, new Text(userPair[0] + "," + userPair[1] + ",IN"));
                }
            }
        }
    }

    public static class FollowerReducer extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(final Text userId, final Iterable<Text> relations, final Context context) throws IOException, InterruptedException {

            List<String> followers = new ArrayList<>();
            List<String> followings = new ArrayList<>();

            for (final Text relation : relations) {
                String[] parts = relation.toString().split(",");
                if (parts[2].equals("IN")) {
                    followers.add(parts[0]);
                } else {
                    followings.add(parts[1]);
                }
            }

            for (String follower : followers) {
                for (String following : followings) {
                    if (!follower.equals(following)) {
                        context.write(NullWritable.get(), new Text(follower + "," + userId.toString() + "," + following));
                    }
                }
            }
        }
    }

    public static class PathToEdgeMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyZX = new Text();
        private final Text valueRecord = new Text();

        @Override
        protected void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
            String[] fields = inputValue.toString().split(",");

            keyZX.set(new Text(fields[2] + "," + fields[0]));
            valueRecord.set(new Text(inputValue.toString() + ",fromPath"));
            context.write(keyZX, valueRecord);
        }
    }

    public static class EdgeJoinMapper extends Mapper<Object, Text, Text, Text> {

        private final Text edgeRecord = new Text();

        @Override
        protected void map(final Object inputKey, final Text inputValue, final Context context) throws IOException, InterruptedException {
            final String[] edgeFields = inputValue.toString().split(",");

            if (Integer.parseInt(edgeFields[0]) < MAX_USERS && Integer.parseInt(edgeFields[1]) < MAX_USERS) {
                edgeRecord.set(new Text(inputValue.toString() + ",fromEdges"));
                context.write(inputValue, edgeRecord);
            }
        }
    }

    public static class TriangleReducer extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        protected void reduce(Text joinKey, Iterable<Text> joinValues, Context context) throws IOException, InterruptedException {
            long path2EdgesCount = 0;
            boolean isTriangleEdge = false;

            for (Text value : joinValues) {
                String[] splitValue = value.toString().split(",");
                if (splitValue.length == 3) {
                    isTriangleEdge = true;
                } else {
                    path2EdgesCount++;
                }
            }

            if (isTriangleEdge) {
                context.getCounter(COUNTERS.TRIANGLE_COUNT).increment(path2EdgesCount);
            }
        }
    }

    private int executePath2Job(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration conf = getConf();
        final Job jobPath2 = Job.getInstance(conf, "Path2Job");

        jobPath2.setJarByClass(TwitterTriangleCountMain.class);

        MultipleInputs.addInputPath(jobPath2, new Path(inputPath + "/edges.csv"),
                TextInputFormat.class, FollowerMapper.class);
        jobPath2.setReducerClass(FollowerReducer.class);

        jobPath2.setMapOutputKeyClass(Text.class);
        jobPath2.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(jobPath2, new Path(outputPath + "/Intermediate"));
        return jobPath2.waitForCompletion(true) ? 0 : 1;
    }

    private int executeTriangleJob(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration conf = getConf();
        final Job jobTriangle = Job.getInstance(conf, "TriangleJob");

        jobTriangle.setJarByClass(TwitterTriangleCountMain.class);

        MultipleInputs.addInputPath(jobTriangle, new Path(inputPath + "/edges.csv"),
                TextInputFormat.class, EdgeJoinMapper.class);
        MultipleInputs.addInputPath(jobTriangle, new Path(outputPath + "/Intermediate"),
                TextInputFormat.class, PathToEdgeMapper.class);

        jobTriangle.setReducerClass(TriangleReducer.class);

        jobTriangle.setMapOutputKeyClass(Text.class);
        jobTriangle.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(jobTriangle, new Path(outputPath + "/FinalResult"));
        jobTriangle.waitForCompletion(true);

        Counters counters = jobTriangle.getCounters();
        Counter triangleCounter = counters.findCounter(COUNTERS.TRIANGLE_COUNT);
        System.out.println(triangleCounter.getDisplayName() + ":" + triangleCounter.getValue());

        return 1;
    }

    @Override
    public int run(final String[] args) throws Exception {
        if (this.executePath2Job(args[0], args[1]) == 0) {
            this.executeTriangleJob(args[0], args[1]);
        }
        return 0;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new TwitterTriangleCountMain(), args);
        } catch (final Exception e) {
            log.error("", e);
        }
    }
}
