package com.project.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class baseMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");

        if (columns.length >= 3) { //okunan lineda veri var mı 
            String productId = columns[1].trim();
            String ratingColumn = columns[2].trim();

            if (Utils.isNumeric(ratingColumn)) {
                
                double rating = Double.parseDouble(ratingColumn);

                context.write(new Text(productId.toString()), new DoubleWritable(rating));
            }
        }
    }
}
