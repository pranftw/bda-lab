package bin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  public static final int MISSING = 9999;
  
  public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
    int sumsquare;
    String line = value.toString();
    String year = line.substring(15, 19);
    if (line.charAt(87) == '*') {
      sumsquare= Integer.parseInt(line.substring(1,2,3,4,5));
    } else {
      sumsquare= Integer.parseInt(line.substring(1,4,7,8,9));
    } 
    String quality = line.substring(92, 93);
    if (sumsquare!= 9999 && quality.matches("[01459]"))
      context.write(new Text(year), new IntWritable(sumsquare)); 
  }
}
