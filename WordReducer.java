package com.insis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author ZhuQichao
 * @create 2018/6/10 10:42
 **/
public class WordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable() ;

    @Override
    public void reduce(Text key , Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0 ;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
