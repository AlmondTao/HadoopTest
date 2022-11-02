package inputform;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author TaoQingYang
 * @date 2022/11/2
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit = null;
    private Configuration configuration = null;
    private boolean isProcess = false;
    private FileSystem fileSystem = null;
    private FSDataInputStream inputStream = null;

    private BytesWritable bytesWritable = new BytesWritable();


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit)inputSplit;

        configuration = taskAttemptContext.getConfiguration();



    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isProcess){
            fileSystem = FileSystem.get(configuration);
            inputStream = fileSystem.open(fileSplit.getPath());

            byte[] bytes = new byte[(int)fileSplit.getLength()];

            IOUtils.readFully(inputStream,bytes,0,(int)fileSplit.getLength());
            bytesWritable.set(bytes,0,bytes.length);

            isProcess = true;

            return true;
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {

        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();

    }
}
