package org.wliu.mr1.example.mulinput;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool {

    public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] sp = value.toString().split(";");
                String userid = sp[0];
                outkey.set(userid);
                outvalue.set("A" + value.toString());
                context.write(outkey, outvalue);
            } catch (Exception e) {
                context.getCounter("UserJoinMapper", "errorlog").increment(1);
            }
        }
    }

    public static class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] sp = value.toString().split(";");
                String userid = sp[0];
                outkey.set(userid);
                outvalue.set("B" + value.toString());
                context.write(outkey, outvalue);
            } catch (Exception e) {
                context.getCounter("UserJoinMapper", "errorlog").increment(1);
            }
        }
    }

    public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text("");
        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        private String joinType = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            joinType = context.getConfiguration().get("join.type");
        }

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            listA.clear();
            listB.clear();
            for (Text value : values) {
                if (value.charAt(0) == 'A') {
                    listA.add(new Text(value.toString().substring(1)));
                } else if (value.charAt(0) == 'B') {
                    listB.add(new Text(value.toString().substring(1)));
                }
            }

            executeJoinLogic(context);
        }

        //根据连接类型做不同处理
        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {
                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    }
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {
                for (Text A : listA) {
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        context.write(A, EMPTY_TEXT);
                    }
                }
            } else if (joinType.equalsIgnoreCase("rightouter")) {
                for (Text B : listB) {
                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            context.write(A, B);
                        }
                    } else {
                        context.write(EMPTY_TEXT, B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("fullouter")) {
                if (!listA.isEmpty()) { 
                    for (Text A : listA) {
                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                context.write(A, B);
                            }
                        }else{
                            context.write(A, EMPTY_TEXT);
                        }
                    }
                } else {
                    for (Text B : listB) {
                       context.write(EMPTY_TEXT, B);
                    }
                }
            } else if (joinType.equalsIgnoreCase("anti")) {
                if (listA.isEmpty() ^ listB.isEmpty()) {
                    for (Text A : listA) {
                        context.write(A, EMPTY_TEXT);
                    }
                    for (Text B : listB) {
                        context.write(EMPTY_TEXT, B);
                    }
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("join.type", "inner");

        
        //设置不同map处理不同输入
        
        // local mode
//        String in1 = "/home/wliu/Desktop/temp/in1.txt";
//        String in2 = "/home/wliu/Desktop/temp/in2.txt";
//        String output = "hdfs://wliu-work:9000/user/wliu/output/out3";
        
        // remote mode
        String in1 = "/user/wliu/multiple/input/in/in1.txt";
        String in2 = "/user/wliu/multiple/input/in/in2.txt";
        String output = "/user/wliu/output/";        
        FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");
        conf.set("mapred.job.tracker", "wliu-work:9001");
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(output), true);
        // remote mode end

        Job job = new Job(conf, "ReduceSideJoin1");
        job.setJarByClass(ReduceSideJoin.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(in1), TextInputFormat.class, UserJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(in2), TextInputFormat.class, CommentJoinMapper.class);
        
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setReducerClass(UserJoinReducer.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try {
//            if (args.length < 4) {
//                System.err.println("ERROR: Parameter format length ");
//                System.exit(0);
//            }
            int ret = ToolRunner.run(new ReduceSideJoin(), args);
            System.exit(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}