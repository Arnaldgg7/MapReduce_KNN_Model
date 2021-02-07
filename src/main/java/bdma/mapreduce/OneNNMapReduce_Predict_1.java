package bdma.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneNNMapReduce_Predict_1 extends JobMapReduce {
	
	private static int N = 100;

	public static class OneNNMapper_Predict_1 extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Getting the parameters that we are going to make use of:
			String type = context.getConfiguration().getStrings("type")[0];
			String train = context.getConfiguration().getStrings("train")[0];
			String test = context.getConfiguration().getStrings("test")[0];

			// Getting all values to fetch the train/test rows:
			String[] arrayValues = value.toString().split(",");
			String rowType = Utils_1NN.getAttribute(arrayValues, type);

			// Perform the cartesian product to get all possible combinations, but taking into
			// account that the Test rows are less in the overall dataset, so they must be used
			// in the 'if-clause' that we are replicating 'N' times in order to minimize the
			// communication overhead over the cluster:
			if (rowType.equals(train)) {
				int newKey = (int)(Math.random()*N);
				context.write(new IntWritable(newKey), value);
			}
			else if (rowType.equals(test)) {
				for (int newKey = 0; newKey < N; newKey++) {
					context.write(new IntWritable(newKey), value);
				}
			}
		}
	}
	
	public static class OneNNReducer_Predict_1 extends Reducer<IntWritable, Text, NullWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String type = context.getConfiguration().getStrings("type")[0];
			String train = context.getConfiguration().getStrings("train")[0];

			// Getting the rows for each 'N' value and store them in 2 ArrayLists depending on
			// the value of 'train' attribute they take:
			ArrayList<String> trainRows = new ArrayList<String>();
			ArrayList<String> testRows = new ArrayList<String>();

			for (Text row : values) {
				String[] arrayValues = row.toString().split(",");
				String rowType = Utils_1NN.getAttribute(arrayValues, type);
				// Let's add them in one or another list depending on the previous value:
				if (rowType.equals(train)) {
					trainRows.add(row.toString());
				} else {
					testRows.add(row.toString());
				}
			}

			// Let's finally compute the cartesian product of those 2 lists:
			for (int i = 0; i < testRows.size(); i++) {
				for (int j = 0; j < trainRows.size(); j++) {
					context.write(NullWritable.get(), new Text(testRows.get(i) + "<->" + trainRows.get(j)));
				}
			}
		}
	}
	
	public OneNNMapReduce_Predict_1() {
		this.input = null;
		this.output = null;
	}

	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "1NN_1");
		OneNNMapReduce_Predict_1.configureJob(job,this.input,this.output);
        return job.waitForCompletion(true);
	}

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(OneNNMapReduce_Predict_1.class);

        // Set the mapper class it must use
        job.setMapperClass(OneNNMapper_Predict_1.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Set the reducer class it must use
        job.setReducerClass(OneNNReducer_Predict_1.class);

        // The output will be Text
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        /**
	    * Specify here the parameters to send to the job
	    **/
		job.getConfiguration().setStrings("type", "train");
		job.getConfiguration().setStrings("train", "1");
		job.getConfiguration().setStrings("test", "0");

        // The files the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }



}
