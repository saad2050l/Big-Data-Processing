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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;


public class ComputeAverage {
    
    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable offset, Text line, Context context)
	    throws IOException, InterruptedException {
		String linee = line.toString();
		if (linee.startsWith("u"))
			return;
		String[] param = linee.split("[,]");
		String movieId = param[1];
		String rating = param[2];
		float value = Float.parseFloat(rating);
	    context.write(new Text (movieId),new FloatWritable (value));
		}
    }
    
    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void reduce(Text movie, Iterable<FloatWritable> values, Context context) 
	    throws IOException, InterruptedException {

		float sum = 0;
		float len = 0;
		for(FloatWritable v : values) {
			sum += v.get();
			len += 1;
		}
	    context.write(new Text (movie),new FloatWritable (sum/len));
	}
    }
}


