package transformation;

public class TFM_Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "TFM_MapReduce");

        Path graphInputPath = new Path("s3://cs6240-demo-bucket-hdfs-hwijong/input/Graph/");
        Path ranksInputPath = new Path("s3://cs6240-demo-bucket-hdfs-hwijong/input/Ranks.csv");
        Path outputPath = new Path("s3://cs6240-demo-bucket-hdfs-hwijong/output/");

        job.setJarByClass(TFM_Driver.class);
        MultipleInputs.addInputPath(job, graphInputPath, TextInputFormat.class, TFM_Mapper.GraphMapper.class);
        MultipleInputs.addInputPath(job, ranksInputPath, TextInputFormat.class, TFM_Mapper.RanksMapper.class);

        job.setReducerClass(TFM_Reducer.JoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}