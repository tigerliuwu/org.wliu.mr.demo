package org.wliu.localmode.eg1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value,Context context) throws java.io.IOException ,InterruptedException {

        String str[] = value.toString().split(" ");
        for(int i =0; i<str.length;i++){
            context.write(new Text(str[i]), new Text());
        }
    };
}