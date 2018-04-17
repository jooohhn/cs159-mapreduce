package spotify;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRdriver extends Configured implements Tool {
  @SuppressWarnings("deprecation")
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf(), "spotify.MRdriver");
    job.setJarByClass(MRdriver.class);
    job.setMapperClass(MRmapper.class);
    job.setReducerClass(MRreducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: MRdriver <input-path> <output-path>");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", "\n");
    System.exit(ToolRunner.run(conf, new MRdriver(), args));
  }
}
