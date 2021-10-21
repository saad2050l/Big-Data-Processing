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


public class SelectPairs {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text line, Context context)
	    throws IOException, InterruptedException {
			String linee = line.toString();
			String[] par = linee.split("\t");
			String movieIds = par[0];
			String count = par[1];
			String[] lstMovieIds = movieIds.split("[,]");
			String movie1 = lstMovieIds[0];
			String movie2 = lstMovieIds[1];
			String value = movie2 + "," + count;
			context.write(new Text (movie1), new Text (value));
	}
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {	
	@Override
	public void reduce(Text movie, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
		// Searching in values for the movie with most count
		
		String value = values.iterator().next().toString();
		String[] para = value.split("[,]");
		String movieId_max = para[0];
		String counti = para[1];
		float count_max = Float.parseFloat(counti);
        for (Text v : values) {
			String valuee = v.toString();
			String[] par = valuee.split("[,]");
			String movieId_cur = par[0];
			String count = par[1];
			float count_cur = Float.parseFloat(count);
			if (count_cur>count_max) {
				movieId_max = movieId_cur;
				count_max = count_cur;
			}
		// exp return : movieId_5 movieId_9 where movieId_9 is the movie with the highest count
		context.write(new Text (movie), new Text (movieId_max));
	}
			
	}
    }
}


