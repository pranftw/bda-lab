package bin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
    int sum_square= 0;
    int count = 0;
    for (IntWritable value : values) {
      sum_square+= value.get();
      
      
      count++;
    } 
    context.write(key, new IntWritable(count*count));
  }
}
