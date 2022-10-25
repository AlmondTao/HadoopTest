package flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author TaoQingYang
 * @date 2022/10/25
 */
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());

        job.setJarByClass(JobMain.class);

        job.setJobName("MyFlowTest");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://192.168.2.100:8020/flow/input"));

        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowWritable.class);

        job.setPartitionerClass(FlowPartition.class);
        job.setCombinerClass(FlowCombiner.class);

        job.setNumReduceTasks(4);
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(FlowWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.2.100:8020/flow/output"));

        boolean b = job.waitForCompletion(true);

        return b?1:0;
    }

    public static void main(String[] args) throws Exception {
        JobMain job = new JobMain();

        int run = ToolRunner.run(new Configuration(), job, args);

        System.exit(run);

    }
}
