package com.insis;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;
import java.util.List;


/**
 * @author ZhuQichao
 * @create 2018/6/10 10:12
 **/
public class WordMain {


    public static void main(String[] args) throws Exception {
        //如果配置好环境变量，没有重启机器，然后报错找不到hadoop.home  可以手动指定
        // System.setProperty("hadoop.home.dir","E:\\hadoop\\hadoop-2.7.4");

//        List<String> lists = Arrays.asList("E:\\input", "E:\\output");
        List<String> lists = Arrays.asList("./ex1_data1", "./test");
        Configuration configuration = new Configuration();

        Job job = new Job(configuration, "word count");

        job.setJarByClass(WordMain.class); // 主类
        job.setMapperClass(WordMapper.class); // Mapper
        job.setCombinerClass(WordReducer.class); //作业合成类
        job.setReducerClass(WordReducer.class); // reducer
        job.setOutputKeyClass(Text.class); // 设置作业输出数据的关键类
        job.setOutputValueClass(IntWritable.class); // 设置作业输出值类

        FileInputFormat.addInputPath(job, new Path(lists.get(0))); //文件输入
        FileOutputFormat.setOutputPath(job, new Path(lists.get(1))); // 文件输出

        System.exit(job.waitForCompletion(true) ? 0 : 1); //等待完成退出
    }
}

