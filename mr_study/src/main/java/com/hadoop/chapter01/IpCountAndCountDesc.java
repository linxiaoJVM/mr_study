package com.hadoop.chapter01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by linxiao on 2016/1/26.
 */
public class IpCountAndCountDesc {
    public static void main(String [] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ip count");

        job.setJarByClass(IPCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(IPCount.IPCountMapper.class);
        job.setReducerClass(IPCount.IPCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); //数据输入目录
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));//输出目录，运算结果保存的路径

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);

        conf = new Configuration();
        job = Job.getInstance(conf, "ip count desc");

        job.setJarByClass(IPCountDesc.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(IPCountDesc.IPCountDesMapperDesc.class);
        job.setReducerClass(IPCountDesc.IPCountReducerDesc.class);
        //mapper 输出key的类型和value的类型
        job.setMapOutputKeyClass(IPCountDesc.IntWritableDesc.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IPCountDesc.IntWritableDesc.class);

        SequenceFileInputFormat.addInputPath(job, new Path(args[1])); //数据输入目录
        FileOutputFormat.setOutputPath(job, new Path(args[2]));//输出目录，运算结果保存的路径

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
