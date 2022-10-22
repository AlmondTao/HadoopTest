package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author TaoQingYang
 * @date 2022/10/20
 */
public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(),JobMain.class.getSimpleName());

        job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);

//        TextInputFormat.addInputPath(job,new Path("hdfs://192.168.2.100:8020/wordcount/input"));

        TextInputFormat.addInputPath(job,new Path("file:///D:\\wordcount\\input\\wordcount.txt"));

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //

        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

//        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.2.100:8020/wordcount/output"));

        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\wordcount\\output"));

        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        JobMain jobMain = new JobMain();

        int run = ToolRunner.run(configuration,jobMain, args);

        System.exit(run);
    }


}
