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


public class ExtractUserMovies {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text line, Context context)
	    throws IOException, InterruptedException {
		String linee = line.toString();
		if (linee.startsWith("u"))
			return;
		String[] param = linee.split("[,]");
		String userId = param[0];
		String movieId = param[1];
		String rating = param[2];
		float rat = Float.parseFloat(rating);
		// Condition : movie rated as good by user
		if (rat > 3 ) {
			// exp return : userId_1 movieId_5
			context.write(new Text (userId), new Text (movieId));
		}
		}
    }
    
    public static class Reduce extends Reducer<Text, FloatWritable, Text, Text> {

	public void reduce(Text userId, Iterable<Text> movieIds, Context context) 
	    throws IOException, InterruptedException {
		String value = "";
        for (Text m : movieIds) {
            String movieId = m.toString();
			value += movieId + ",";
		String val = value.substring(0,value.length()-1);
		// exp return : userId_1 movieId_5,movieId_9,movieId_10
		context.write(new Text (userId), new Text (val));
	}
    }
}
}

