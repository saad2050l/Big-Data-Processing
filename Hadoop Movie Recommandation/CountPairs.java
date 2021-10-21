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


public class CountPairs {
    
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable offset, Text line, Context context)
	    throws IOException, InterruptedException {
			String linee = line.toString();
			String[] par = linee.split("\t");
			String movieIds = par[1];
			String[] lstMovieIds = movieIds.split(",");
			for (String x : lstMovieIds) {
				for (String y : lstMovieIds) {
					if ( !y.equals(x) ) { // movie x and y have to be different
						String key = x+","+y;
						int value = 1;
						// returning every distinct couple of movies 
						// that were rated as 'good' according to the user and 1
						context.write(new Text (key), new IntWritable (value));
					}
				}
			}
	}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {	
	public void reduce(Text movie, Iterable<IntWritable> values, Context context) 
	    throws IOException, InterruptedException {
		int len = 0;
		for(IntWritable v : values) {
			len += 1;
		}
		// return : movieId_x,movieId_y count
	    context.write(new Text (movie),new IntWritable (len));
	}
    }
}


