package com.hadoop.chapter02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by lin on 2016/2/2.
 *
 * reduce side join方式
 *
 * user.txt
 * 客户ID    姓名
 1    AllenStandard
 2    SmithPremium
 3    StevensStandard
 4    HafezPreimum

 sales.txt
 客户ID    金额    时间
 1    35.9    2010-01-25
 2    23.2    2013-04-11
 4    50.0    2012-11-02
 3    68.99    2012-04-10
 1    36.89   2011-02-01
 2    78.99    2012-06-21
 */
public class SalesStatReduceJoin {

    public static class NamesMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context){
            String words [] = value.toString().split("\t");
            try {
                context.write(new IntWritable(Integer.valueOf(words[0])), new Text("name"+words[1]));

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public static class SalesMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context){
            String words [] = value.toString().split("\t");
            try {
                context.write(new IntWritable(Integer.valueOf(words[0])), new Text("sale"+words[1]));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class NameAndSaleReduceJoin extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) {
            String name = "";
            Set<Double> price = new TreeSet<Double>();

            for(Text val : values) {
                //name.txt数据
               if(val.toString().startsWith("name")) {
                   name = val.toString().substring(4);
               }else {
                   price.add(Double.valueOf(val.toString().substring(4)));
               }

            }
            //写入hdfs中。注意的是，这里会出现一个性能问题，假如当前这个key的values数量非常大，也就是说一个用户销售记录非常多，
            //以至于当前节点的内存放不下，会出现错误，需要改进
            for(Double val : price) {
                try {
                    context.write(new Text(name), new DoubleWritable(val));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sales stat");

        job.setJarByClass(SalesStatReduceJoin.class);
        //指定reducer
        job.setReducerClass(NameAndSaleReduceJoin.class);

        //设置map输出格式
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(Text.class);

        //reducer输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //使用 MultipleOutputs，处理多个数据来源
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NamesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SalesMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
