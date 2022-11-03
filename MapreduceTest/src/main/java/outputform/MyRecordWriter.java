package outputform;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author TaoQingYang
 * @date 2022/11/3
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream goodCommentsOutputStream;
    private FSDataOutputStream badCommentOutputStream;

    public MyRecordWriter(FSDataOutputStream goodCommentsOutputStream, FSDataOutputStream badCommentOutputStream) {
        this.goodCommentsOutputStream = goodCommentsOutputStream;
        this.badCommentOutputStream = badCommentOutputStream;
    }

    public MyRecordWriter() {
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String[] split = text.toString().split("\t");
        String numberStr = split[9];

        if (Integer.parseInt(numberStr)<=1){
            goodCommentsOutputStream.write(text.toString().getBytes());
            goodCommentsOutputStream.write("\r\n".getBytes());

        }else{
            badCommentOutputStream.write(text.toString().getBytes());
            badCommentOutputStream.write("\r\n".getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        goodCommentsOutputStream.close();
        badCommentOutputStream.close();
    }
}
