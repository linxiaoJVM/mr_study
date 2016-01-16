package com.hadoop.chapter01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IPCount {
	//maper
	public static class IPCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context){
			String words [] = value.toString().split(" ");
			try {
				word.set(words[0]);
				context.write(word, one);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//reduce
	public static class IPCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) {
			int total = 0;
			for(IntWritable val : values) {
				total += val.get();
			}
			try {
				context.write(key, new IntWritable(total));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String [] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ip count");
		
		job.setJarByClass(IPCount.class);
		
		job.setMapperClass(IPCountMapper.class);
//		job.setCombinerClass(IPCountReducer.class);
		job.setReducerClass(IPCountReducer.class);
		//job.setSortComparatorClass(cls);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); //数据输入目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出目录，运算结果保存的路径
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// 	175.44.30.93 - - [29/Sep/2013:00:10:16 +0800] "GET /structure/heap/ HTTP/1.1" 200 22539 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1;)" 

	}

}
