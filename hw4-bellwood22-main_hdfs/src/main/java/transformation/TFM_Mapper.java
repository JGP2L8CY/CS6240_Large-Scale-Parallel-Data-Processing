package transformation;

// import
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.IOException;

// function
public class TFM_Mapper {

    // graph
    public static class GraphMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            String[] parts=value.toString().split(",");
            int pageId=Integer.parseInt(parts[0]);
            int link=Integer.parseInt(parts[1]);
            context.write(new IntWritable(pageId), new Text("L" + link));

            if (link==0) {
                context.write(new IntWritable(-1),new Text("D"+pageId));
            }
        }
    }

    // ranks
    public static class RanksMapper extends Mapper<LongWritable, Text, IntWritable,Text>{
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String[] parts=value.toString().split(",");
            int pageId=Integer.parseInt(parts[0]);
            double rank=Double.parseDouble(parts[1]);
            context.write(new IntWritable(pageId), new Text("R" + rank));
        }
    }

}