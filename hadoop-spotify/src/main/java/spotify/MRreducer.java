package spotify;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

public class MRreducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  
  /**
   * @key: artist
   * @values: List of "trackname,streams"
   */
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

    // 1: initialize variables
    long streams = values.iterator().next().get();

    // 2: Aggregate stream count
    for (LongWritable value : values) 
      streams += value.get();
    
    // 3: Write to output
    context.write(new Text("artist: " + key.toString()), new LongWritable(streams));
  }
}
