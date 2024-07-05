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

	public class Range {
	    

	    public static class JobMapper extends baseMapper {}

	    public static class JobReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

	            // Count min and max.
	            double max = Double.MIN_VALUE;
	            double min = Double.MAX_VALUE;

	            for (DoubleWritable val : values) {
	                double value = val.get();
	                max = Math.max(max, value);
	                min = Math.min(min, value);
	            }

	            context.write(key, new DoubleWritable(max - min));
	        }
	    }
	    @SuppressWarnings("deprecation")
	    public static void main(String[] args) throws Exception

	    {

	        Configuration conf = new Configuration();



	      

	        Job job = new Job(conf, "Range_ratings");

	        job.setJarByClass(Range.class);



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
