package com.hadoop.chapter02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lin on 2016/2/2.
 * reduce side join方式
 * 升级版本，解决内存占用过大问题
 *  * user.txt
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
public class SalesStatReduceJoinUpgrade {
    /**
     * 自定义key
     */
    public static class TwoFieldKey implements WritableComparable<TwoFieldKey> {
        private IntWritable one;
        private DoubleWritable two;

        //默认构造函数，不写会报空指针错误
        public TwoFieldKey(){
            this.one = new IntWritable();
            this.two = new DoubleWritable();
        }
        public TwoFieldKey(int one, double two) {
            this.one = new IntWritable(one);
            this.two = new DoubleWritable(two);
        }

        public int getOne() {
            return one.get();
        }
        public double getTwo() {
            return two.get();
        }

        @Override
        public int compareTo(TwoFieldKey o) {
           if(this.getOne() > o.getOne()) {return 1;}
           else if(this.getOne() < o.getOne()) {return -1;}
           else {
               return ( this.getTwo() == o.getTwo() ) ? 0 : (this.getTwo() > o.getTwo() ? 1 : -1);
           }
        }

        @Override
        public boolean equals(Object obj) {
            if(obj instanceof TwoFieldKey) {
                TwoFieldKey r = (TwoFieldKey) obj;
                return r.getOne() == ((TwoFieldKey) obj).getOne();
            }else {
                return false;
            }
        }
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.one.write(dataOutput);
            this.two.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.one.readFields(dataInput);
            this.two.readFields(dataInput);
        }
    }

    /**
     * 自定义分区方法，按照TwoFieldKey中的one分区
     */
    public static class OnePartitioner extends Partitioner<TwoFieldKey, Text> {
        @Override
        public int getPartition(TwoFieldKey twoFieldKey, Text text, int numPartitions) {
            return twoFieldKey.getOne() % numPartitions;
        }
    }

    /**
     * 自定义排序类
     */
    public static class OneGroupingComparator extends WritableComparator {
        public OneGroupingComparator() {
            super(TwoFieldKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            int one = ((TwoFieldKey)a).getOne();
            int two = ((TwoFieldKey)b).getOne();
            return one > two ? 1 : (one == two ? 0 : -1);
        }
    }

    public static class NameMapper extends Mapper<Object, Text, TwoFieldKey, Text> {
        public void map(Object key, Text value, Context context){
            String words [] = value.toString().split("\t");
//            System.out.println("user.txt "+words[0]+" "+words[1]);
            try {
                context.write(new TwoFieldKey(Integer.valueOf(words[0]), -1), new Text(words[1]));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public static class SalesMapper extends Mapper<Object, Text, TwoFieldKey, Text> {
        public void map(Object key, Text value, Context context){
            String words [] = value.toString().split("\t");
//            System.out.println("sales.txt "+words[0]+" "+words[1]);
            try {
                context.write(new TwoFieldKey(Integer.valueOf(words[0]), Double.valueOf(words[1])), new Text(words[1]));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class NameAndSaleReduceJoinUp extends Reducer<TwoFieldKey, Text, Text, Text> {
        public void reduce(TwoFieldKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text name = null;
            for(Text val : values) {
                if(name == null) {
                    name = new Text(val);
                    continue;
                }
                context.write(name, val);

            }
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sales stat");

        job.setJarByClass(SalesStatReduceJoinUpgrade.class);
        //指定reducer
        job.setReducerClass(NameAndSaleReduceJoinUp.class);

        //设置map输出格式
        job.setMapOutputKeyClass(TwoFieldKey.class);
        job.setMapOutputValueClass(Text.class);

        //reducer输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(OnePartitioner.class);
        //如果没有通过job.setSortComparatorClass设置key比较函数类，则使用key的实现的compareTo方法
//        job.setSortComparatorClass();
        job.setGroupingComparatorClass(OneGroupingComparator.class);

        //使用 MultipleOutputs，处理多个数据来源
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NameMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SalesMapper.class);

        Path out_path = new  Path(args[2]);
        FileSystem file = out_path.getFileSystem(conf);
        file.delete(out_path, true);

        FileOutputFormat.setOutputPath(job, out_path);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
