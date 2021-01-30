package bdma.mapreduce;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class OneNNMapReduce_Predict_2 extends JobMapReduce {

	public static class OneNNMapper_Predict_2 extends Mapper<LongWritable, Text, Text, Text> {

        private static ArrayList<String> predictors = Utils_1NN.getPredictors();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Getting the required parameters sent to the configuration of the job:
			String diagnosis = context.getConfiguration().getStrings("diagnosis")[0];
			String id = context.getConfiguration().getStrings("id")[0];

        	// Getting the Test part and the Train part for each input value:
			String[] testPart = value.toString().split("<->")[0].split(",");
			String[] trainPart = value.toString().split("<->")[1].split(",");

			// Getting the relevant values to output in the Map function for each test/train part:
			String diagn_test_val = Utils_1NN.getAttribute(testPart, diagnosis);
			String diagn_train_val = Utils_1NN.getAttribute(trainPart, diagnosis);
			String id_test_val = Utils_1NN.getAttribute(testPart, id);

			// Computing the Euclidean Distance between the Test and the Train predictor columns:
			predictors = Utils_1NN.getPredictors();
			double eucl_dist = 0;
			for (int i = 0; i < predictors.size(); i++) {
				Double test_val = Double.valueOf(Utils_1NN.getAttribute(testPart, predictors.get(i)));
				Double train_val = Double.valueOf(Utils_1NN.getAttribute(trainPart, predictors.get(i)));
				eucl_dist += Math.pow(test_val-train_val, 2);
			}

			// We write the output key-values, keeping track of the 'id' attribute of the Test part
			// in order to use it in the Reduce function to check the minimum euclidean distance
			// among all values under the same reduce input key:
			context.write(new Text(id_test_val), new Text(diagn_test_val
					+"<->"+diagn_train_val+","+Math.sqrt(eucl_dist)));
		}

	}

	public static class OneNNReducer_Predict_2 extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Creating a variable to store the minimum and the test and train attributes
			// from the structure we defined in the Map output in which the minimum was found:
			Double min = null;
			String testVal = null;
			String[] trainVals = null;

			// Getting the Test part and the Train part that we sent in the value, for each of
			// the values the MapReduce framework gathered in the 'values', and overwriting the
			// variables 'testVal' and 'trainVals' only with the minimum euclidean distance
			// found so far:
			for (Text value : values) {
				String testPart = value.toString().split("<->")[0];
				String[] trainPart = value.toString().split("<->")[1].split(",");
				double eucl_dist = Double.parseDouble(trainPart[1]);
				if (min != null && eucl_dist < min) {
					min = eucl_dist;
					testVal = testPart;
					trainVals = trainPart;
				}
				if (min == null) {
					min = eucl_dist;
					testVal = testPart;
					trainVals = trainPart;
				}
			}

			// Now, get the diagnosis values for the test and train parts for the value position
			// in which the euclidean distance was the lowest, and output both of them:
			context.write(NullWritable.get(), new Text(trainVals[0]+"_"+testVal));
		}
	}

	public OneNNMapReduce_Predict_2() {
		this.input = null;
		this.output = null;
	}

	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "1NN_1");
		OneNNMapReduce_Predict_2.configureJob(job,this.input,this.output);
        return job.waitForCompletion(true);
	}

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(OneNNMapReduce_Predict_2.class);

        // Set the mapper class it must use
        job.setMapperClass(OneNNMapper_Predict_2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set the reducer class it must use
        job.setReducerClass(OneNNReducer_Predict_2.class);

        // The output will be Text
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        /**
	    * Specify here the parameters to send to the job
	    **/
		job.getConfiguration().setStrings("type", "train");
		job.getConfiguration().setStrings("train", "1");
		job.getConfiguration().setStrings("test", "0");
		job.getConfiguration().setStrings("diagnosis", "diagnosis");
		job.getConfiguration().setStrings("benign", "B");
		job.getConfiguration().setStrings("malign", "M");
		job.getConfiguration().setStrings("id", "id");

        // The files the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }



}
