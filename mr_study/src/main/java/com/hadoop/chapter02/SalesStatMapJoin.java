package com.hadoop.chapter02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by lin on 2016/2/4.
 * map side join方式,使用DistributedCache
 */
public class SalesStatMapJoin {
    public static class SalesMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] localCacheFiles = context.getCacheFiles();
            if(localCacheFiles == null || localCacheFiles.length == 0) {
                System.out.println("localCacheFiles is null");
            }
            System.out.println("Cache length : "+localCacheFiles.length+" "+localCacheFiles[0].toString());

            readCacheFile(context, localCacheFiles[0].toString());
        }
        private static Map<IntWritable, String> nameMap = new HashMap<IntWritable, String>();
        /**
         * 从DistributedCache中读取配置文件，放到内存中
         * @param cacheFileURI
         * @throws IOException
         * http://stackoverflow.com/questions/26560570/filenotfoundexception-while-adding-file-in-cache-hadoop-mapreduce
         *
         */
        private static void readCacheFile(Context context, String cacheFileURI) throws IOException {
//            BufferedReader reader = new BufferedReader(new FileReader(cacheFileURI));
            String line = null;
//            while ((line = reader.readLine()) != null) {
//                System.out.println("name:"+line);
//                String [] split = line.split("\t");
//                nameMap.put(new IntWritable(Integer.valueOf(split[0])), split[1]);
//            }
//            reader.close();

            //You need to use distributed file system, not the local one
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFileURI);
            FSDataInputStream fsin = fs.open(path);
            DataInputStream in = new DataInputStream(fsin);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            while ((line = reader.readLine()) != null) {
                System.out.println("name:"+line);
                String [] split = line.split("\t");
                nameMap.put(new IntWritable(Integer.valueOf(split[0])), split[1]);
            }
            //Use br

            reader.close();
            in.close();
            fsin.close();
        }

        public void map(Object key, Text value, Context context){
            String words [] = value.toString().split("\t");
//            System.out.println("sales.txt "+words[0]+" "+words[1]);
            String name_temp = nameMap.get(new IntWritable(Integer.valueOf(words[0])));
            String name = (name_temp == null) ? "NotName" : name_temp;
            try {
                context.write(new Text(name), new DoubleWritable(Double.valueOf(words[1])));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class NameAndSaleMapJoin extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {

            for(DoubleWritable val : values) {
                try {
                    context.write(key, val);
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

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        job.setJarByClass(SalesStatMapJoin.class);

        job.addCacheFile(new Path("/linxiao/user.txt").toUri());

        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(NameAndSaleMapJoin.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        //reducer输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //数据输入目录

        Path out_path = new  Path(args[1]);
        FileSystem file = out_path.getFileSystem(conf);
        file.delete(out_path, true);

        FileOutputFormat.setOutputPath(job, out_path);
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//输出目录，运算结果保存的路径

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
