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
import java.util.ArrayList;

public class StandardDeviation {
 

    public static class JobMapper extends baseMapper {}

    public static class JobReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            // Convert to ArrayList
            ArrayList<Double> valueList = new ArrayList<>();
            values.iterator().forEachRemaining((DoubleWritable value) -> valueList.add(value.get()));

            // Find mean.
            double sum = valueList.stream().mapToDouble(a -> a).sum();
            double mean = sum / valueList.size();

            // Calculate standard deviation.
            double sumOfSquares = valueList.stream()
                .mapToDouble(a -> Math.pow(mean - a, 2)).sum();

            double variance = sumOfSquares / (valueList.size() - 1);
            double standardDeviation = Math.sqrt(variance);

            context.write(key, new DoubleWritable(standardDeviation));
        }
    }
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception

    {

        Configuration conf = new Configuration();



      

        Job job = new Job(conf, "STD_ratings");

        job.setJarByClass(StandardDeviation.class);



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