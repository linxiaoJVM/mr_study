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
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 在IPCount中，输出的结果结果默认是按key排序的，格式是：
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
 思路是按访问数量做为key并降序排列，ip地址作为value，map处理后应该是这样的
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
 *主要思路是自定义排序的主键类IntWritableDesc，让他倒排序
 */
public class IPCountDesc {
	//maper
	public static class IPCountMapperDesc extends Mapper<Object, Text, IntWritableDesc, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context){
			String words [] = value.toString().split("\t");
			System.out.println(value.toString()+" "+words[0]+" "+words[1]);
			try {
				word.set(words[0]);
				context.write(new IntWritableDesc( Integer.valueOf(words[1])), word);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class IPCountDesMapperDesc extends Mapper<Text, IntWritable, IntWritableDesc, Text> {
		private Text word = new Text();

		public void map(Text key, IntWritable value, Context context){
			try {
				context.write(new IntWritableDesc(value.get()), key);
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
	public static class IPCountReducerDesc extends Reducer<IntWritableDesc, Text, Text, IntWritableDesc> {
		public void reduce(IntWritableDesc key, Iterable<Text> values, Context context) {

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
	public static class IntWritableDesc implements WritableComparable<IntWritableDesc> {
		private int value;
		public IntWritableDesc(){}
		public IntWritableDesc(int value) {this.value = value;}

		public void set(int value) {
			this.value = value;
		}
		public int get() {
			return this.value;
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			dataOutput.writeInt(this.value);
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			this.value = dataInput.readInt();
		}

		@Override
		public int compareTo(IntWritableDesc o) {
			int thisValue = this.value;
			int thatValue = o.value;
			//return thisValue < thatValue ? -1 : (thisValue == thatValue?0:1);
			return thisValue < thatValue ? 1 : (thisValue == thatValue?0:-1);
		}
		public boolean equals(Object o) {
			if(!(o instanceof IntWritableDesc)) {
				return false;
			} else {
				IntWritableDesc other = (IntWritableDesc)o;
				return this.value == other.value;
			}
		}
		public int hashCode() {
			return this.value;
		}
		public String toString() {
			return Integer.toString(this.value);
		}
	}

	public static void main(String [] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ip count desc");
//		conf.set("mapreduce.job.maps", "2");
//		conf.set("mapreduce.job.reduces", "1");

		job.setJarByClass(IPCountDesc.class);
//		DistributedCache.addFileToClassPath();
//		job.addFileToClassPath(new Path(""));
//		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,IPCountMapperDesc.class);

		job.addArchiveToClassPath(new Path(""));

		job.setMapperClass(IPCountMapperDesc.class);
		job.setReducerClass(IPCountReducerDesc.class);
		//mapper 输出key的类型和value的类型
		job.setMapOutputKeyClass(IntWritableDesc.class);
		job.setMapOutputValueClass(Text.class);
		//reduce 输出key的类型和value的类型
		job.setOutputKeyClass(IntWritableDesc.class);
		job.setOutputValueClass(Text.class);

//		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritableDesc.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); //数据输入目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出目录，运算结果保存的路径

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
