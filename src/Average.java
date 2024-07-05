package com.project.bigdata.hadoop;


	

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Average {
    public static Class<?> OutputKeyClass = Text.class;
    

    public static class JobMapper extends baseMapper {}

    public static class JobReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int length = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                length++;
            }

            context.write(key, new DoubleWritable(sum / length));
        }
    }


@SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception

{

    Configuration conf = new Configuration();



  

    Job job = new Job(conf, "Average_ratings");

    job.setJarByClass(Average.class);



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