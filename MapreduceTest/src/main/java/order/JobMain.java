package order;

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
import outputform.MyOutputFormat;
import outputform.MyOutputFormatMapper;

/**
 * @author TaoQingYang
 * @date 2022/10/25
 */
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());

        job.setJarByClass(JobMain.class);

        job.setJobName("MyOrderTest");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\orderTest\\input"));
        job.setNumReduceTasks(3);
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);


        job.setPartitionerClass(OrderPartition.class);
        job.setGroupingComparatorClass(OrderGroupComparator.class);


        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);



        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\orderTest\\output"));

        boolean b = job.waitForCompletion(true);

        return b?1:0;
    }

    public static void main(String[] args) throws Exception {
        JobMain job = new JobMain();

        int run = ToolRunner.run(new Configuration(), job, args);

        System.exit(run);

    }
}
