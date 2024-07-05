package com.project.bigdata.hadoop;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

	public class Median {
		
	   

	    public static class JobMapper extends baseMapper {}

	    public static class JobReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

	            // Convert to ArrayList
	            ArrayList<Double> valueList = new ArrayList<>();
	            values.iterator().forEachRemaining((DoubleWritable value) -> valueList.add(value.get()));

	            // Sort
	            Collections.sort(valueList);

	            // Get median.
	            double median = valueList.get(valueList.size() / 2);

	            context.write(key, new DoubleWritable(median));
	        }
	    }
	    
	    @SuppressWarnings("deprecation")
		public static void main(String[] args) throws Exception

	    {

	        Configuration conf = new Configuration();



	      

	        Job job = new Job(conf, "Median_ratings");

	        job.setJarByClass(Median.class);



	        job.setMapOutputKeyClass(Text.class);

	        job.setMapOutputValueClass(DoubleWritable.class);

	         

	        // job.setNumReduceTasks(0);

	        job.setOutputKeyClass(Text.class);

	        job.setOutputValueClass(DoubleWritable.class);



	        job.setMapperClass(baseMapper.class);

	        job.setReducerClass(JobReducer.class);



	        job.setInputFormatClass(TextInputFormat.class);

	        job.setOutputFormatClass(TextOutputFormat.class);



	        FileInputFormat.addInputPath(job, new Path(args[0]));

	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        Path out = new Path(args[1]);

	        out.getFileSystem(conf).delete(out);

	        job.waitForCompletion(true);

	    }
	}
