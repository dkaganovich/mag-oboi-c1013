package edu.gatech.cse6242;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Task1 extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Task1.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Task1(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "task1");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable node = new IntWritable();
    private IntWritable weight = new IntWritable();

    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {
      String[] tkns = lineText.toString().split("\t");
      node.set(Integer.valueOf(tkns[1]));
      weight.set(Integer.valueOf(tkns[2]));
      context.write(node, weight);
    }
  }

  public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    public void reduce(IntWritable node, Iterable<IntWritable> weights, Context context)
            throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable weight : weights) {
        sum += weight.get();
      }
      context.write(node, new IntWritable(sum));
    }
  }
}