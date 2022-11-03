package outputform;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/11/3
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        FSDataOutputStream goodCommentOutputStream = fileSystem.create(new Path("file:///D:\\myoutput\\good_comments\\good_comments.txt"));
        FSDataOutputStream badCommentOutputStream = fileSystem.create(new Path("file:///D:\\myoutput\\bad_comments\\bad_comments.txt"));

        MyRecordWriter myRecordWriter = new MyRecordWriter(goodCommentOutputStream, badCommentOutputStream);


        return myRecordWriter;
    }

}
