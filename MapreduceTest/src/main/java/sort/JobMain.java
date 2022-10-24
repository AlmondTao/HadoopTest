package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author TaoQingYang
 * @date 2022/10/24
 */
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf, JobMain.class.getSimpleName());

        job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job,new Path("hdfs://192.168.2.100:8020/sort/input"));

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.2.100:8020/sort/out"));

        boolean b = job.waitForCompletion(true);

        return b?1:0;
    }

    public static void main(String[] args) throws Exception {
        JobMain jobMain = new JobMain();
        jobMain.setConf(new Configuration());

        ToolRunner.run(jobMain,args);

    }
}
