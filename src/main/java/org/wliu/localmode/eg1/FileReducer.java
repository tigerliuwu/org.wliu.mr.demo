package org.wliu.localmode.eg1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key,Iterable<Text> value,Context context) throws java.io.IOException ,InterruptedException {

        int count=0;
        for (Text text : value) {
            count++;
        }
        context.write(key, new Text(count+""));
    };
}