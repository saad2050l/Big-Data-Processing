package tpt.dataai922;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.log4j.Logger;


public class MovieRecommendation extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(MovieRecommendation.class);

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new MovieRecommendation(), args);
	System.exit(res);
    }

    public int run(String[] args) throws Exception {

	
	String temp1 = ".a1";
	String temp2 = ".a2";
	String final_out = ".a3";

	
    	JobControl jobControl = new JobControl("MovieRecommendation"); 
	
        Configuration conf1 = getConf();

        // JOB 1 : Extract User Movies
    	Job job1 = Job.getInstance(conf1, "Extract User Movies");
    	job1.setJarByClass(this.getClass());
    	
    	FileInputFormat.addInputPath(job1, new Path(args[0]+"/ratings.csv"));
    	FileOutputFormat.setOutputPath(job1, new Path(args[1]+temp1));
    	
    	job1.setMapperClass(ExtractUserMovies.Map.class);
    	job1.setReducerClass(ExtractUserMovies.Reduce.class);
    	
    	job1.setOutputValueClass(Text.class);
    	job1.setOutputKeyClass(Text.class);
    	
    	ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        
        // add the job to the job control
        jobControl.addJob(controlledJob1);
	
        Configuration conf2 = getConf();
        // JOB 2 : Count Pairs
        Job job2 = Job.getInstance(conf2, "Count Pairs");
    	job2.setJarByClass(this.getClass());
    	
    	FileInputFormat.addInputPath(job2, new Path(args[1]+temp1));
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]+temp2));
    	
    	job2.setMapperClass(CountPairs.Map.class);
    	job2.setReducerClass(CountPairs.Reduce.class);
    	
    	job2.setOutputValueClass(Text.class);
    	job2.setOutputKeyClass(IntWritable.class);
    	
    	ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);
	// make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
	// add the job to the job control
        jobControl.addJob(controlledJob2);

        Configuration conf3 = getConf();
        // JOB 3 : Count Pairs
        Job job3 = Job.getInstance(conf3, "Select Pairs");
    	job3.setJarByClass(this.getClass());
    	
    	FileInputFormat.addInputPath(job3, new Path(args[1]+temp2));
    	FileOutputFormat.setOutputPath(job3, new Path(args[1]+final_out));
    	
    	job3.setMapperClass(SelectPairs.Map.class);
    	job3.setReducerClass(SelectPairs.Reduce.class);
    	
    	job3.setOutputValueClass(Text.class);
    	job3.setOutputKeyClass(IntWritable.class);
    	
    	ControlledJob controlledJob3 = new ControlledJob(conf3);
        controlledJob3.setJob(job3);
	// make job3 dependent on job2
        controlledJob3.addDependingJob(controlledJob2);
	// add the job to the job control
        jobControl.addJob(controlledJob3);

	// Create Thread for the job chain
	Thread jobControlThread = new Thread(jobControl);
    	jobControlThread.start();

	while (!jobControl.allFinished()) {
	    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
	    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
	    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
	    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
	    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
	    try {
    Thread.sleep(5000);
    } catch (Exception e) {

    }

	  } 
   System.exit(0);  
   return (job1.waitForCompletion(true) ? 0 : 1); 

    }    
	
}

