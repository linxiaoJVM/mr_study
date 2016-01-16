package com.hadoop.chapter01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 在IPCount中，输出的结果格式是：
 *  95.26.244.193   1
	95.91.241.66    3
	96.254.171.2    14
	98.245.146.5    4
	99.225.156.100  3
	99.238.64.184   3
	。。。。。。。
	现在需要按访问ip数量从大到小排列输入，如
	96.254.171.2    14
	98.245.146.5    4
	95.91.241.66    3
	99.225.156.100  3
	99.238.64.184   3
	。。。。。
	思路是按访问数量做为key，ip地址作为value，map处理后应该是这样的
	14 ： <96.254.171.2>
	4 ： <98.245.146.5>
	3 ： <95.91.241.66，99.225.156.100，99.238.64.184>
	reduce处理后变成如下格式：
	96.254.171.2    14
	98.245.146.5    4
	95.91.241.66    3
	99.225.156.100  3
	99.238.64.184   3
 * @author linxiao
 *主要思路是自定义排序的主键类IntKeyDesc，让他倒排序
 */
public class IPCountDesc {
	//maper
	public static class IPCountMapperDesc extends Mapper<Object, Text, IntKeyDesc, Text> {
		public void map(Object key, Text value, Context context){
			String words [] = value.toString().split("\t");
			System.out.println(value.toString()+" "+words[0]+" "+words[1]);
			try {
				context.write(new IntKeyDesc(new IntWritable(Integer.valueOf(words[1]))), new Text(words[0]));
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
	public static class IPCountReducerDesc extends Reducer<IntKeyDesc, Text, Text, IntKeyDesc> {
		public void reduce(IntKeyDesc key, Iterable<Text> values, Context context) {
			
			try {
				for(Text val : values) {
					context.write(val, key);
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class IntKeyDesc implements WritableComparable<IntKeyDesc> {

		private IntWritable key;
		public IntKeyDesc() {
			this.key = new IntWritable();
		}
		public IntKeyDesc(IntWritable key) {
			this.key = key;
		}
		public IntWritable getKey() {
			return this.key;
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			this.key.readFields(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			this.key.write(arg0);
		}

		@Override
		public int compareTo(IntKeyDesc o) {
			// TODO Auto-generated method stub
			return -this.key.compareTo(o.getKey());
		}
		//如果不重写，输出结果是显示的当前对象IntKeyDesc的在堆中的地址
		@Override
	    public String toString() {
	        return this.key.toString();
	    }
		
		
	}
	
	public static void main(String [] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ip count desc");
		
		job.setJarByClass(IPCountDesc.class);
		
		job.setMapperClass(IPCountMapperDesc.class);
		job.setReducerClass(IPCountReducerDesc.class);
		
		job.setMapOutputKeyClass(IntKeyDesc.class);
		job.setMapOutputValueClass(Text.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntKeyDesc.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); //数据输入目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出目录，运算结果保存的路径
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	

}
