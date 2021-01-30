package bdma.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainLocal extends Configured implements Tool {
    
    static String HADOOP_COMMON_PATH = "C:\\Users\\Arnald\\Desktop\\ARNALD\\KNOWLEDGE\\PROJECTE MASTER\\MASTER\\Hands-On Experience\\MapReduce\\Lab02-MapReduce-Java_assignment\\Lab02-MapReduce-Java_assignment\\src\\main\\resources\\winutils";

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LocalMapReduce");

        if (args[0].equals("-predict_1")) {
            OneNNMapReduce_Predict_1.configureJob(job, args[1], args[2]);
        }
        else if (args[0].equals("-predict_2")) {
            OneNNMapReduce_Predict_2.configureJob(job, args[1], args[2]);
        }
        else if (args[0].equals("-test")) {
            OneNNMapReduce_Test.configureJob(job, args[1],args[2]);
        }

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
        
        MainLocal driver = new MainLocal();
        int exitCode = ToolRunner.run(driver,args);
        System.exit(exitCode);
    }

}
