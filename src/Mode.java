package com.project.bigdata.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class Mode {
	   

	    public static class JobMapper extends baseMapper {}

	    public static class JobReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

	            // Count occurrences.
	            Map<Double, Integer> occurrences = new HashMap<>();

	            for (DoubleWritable val : values) {
	                double value = val.get();
	                int count = Optional.ofNullable(occurrences.get(value)).orElse(0);
	                occurrences.put(value, count + 1);
	            }

	            // Find mode.
	            double mode = Collections.max(
	                occurrences.entrySet(),
	                Map.Entry.comparingByValue()
	            ).getKey();

	            context.write(key, new DoubleWritable(mode));
	        }
	    }
	    
	    @SuppressWarnings("deprecation")
	    public static void main(String[] args) throws Exception

	    {

	        Configuration conf = new Configuration();



	      

	        Job job = new Job(conf, "Mode_ratings");

	        job.setJarByClass(Mode.class);



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
