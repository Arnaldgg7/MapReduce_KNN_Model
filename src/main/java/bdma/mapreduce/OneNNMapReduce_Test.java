package bdma.mapreduce;

import java.io.IOException;

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

public class OneNNMapReduce_Test extends JobMapReduce {
	
	public static class OneNNMapper_Test extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Just outputting the value of each line of the file, which is one of the four
			// possible values along with a '1', so that the Combiner Class and the Reduce function
			// can add up all of '1' with the same key, yielding our expected output:
			context.write(value, new IntWritable(1));
		}
		
	}
	
	public static class OneNNReducer_Test extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// Iterating over the values per key and adding all of them up:
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(new Text(key), new IntWritable(count));
		}
	}
	
	public OneNNMapReduce_Test() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "1NN_2");
       		OneNNMapReduce_Test.configureJob(job,this.input,this.output);
		return job.waitForCompletion(true);
	}

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(OneNNMapReduce_Test.class);

        // Set the mapper class it must use
        job.setMapperClass(OneNNMapper_Test.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set a combiner
        job.setCombinerClass(OneNNReducer_Test.class);

        // Set the reducer class it must use
        job.setReducerClass(OneNNReducer_Test.class);

        // The output will be Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // The files the job will read from/write to
        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
    }

}
