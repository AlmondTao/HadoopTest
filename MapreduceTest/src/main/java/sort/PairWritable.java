package sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author TaoQingYang
 * @date 2022/10/24
 */
public class PairWritable implements WritableComparable<PairWritable> {

    String first;

    int second;


    public PairWritable() {
    }

    public PairWritable(String first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(PairWritable o) {
        System.out.print(this.toString());
        System.out.print(o.toString());

        int i = this.first.compareTo(o.first);

        if (i != 0 ){
            return i;

        }

        return this.second > o.second? 1:-1;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }

    @Override
    public String toString() {
        return "PairWritable{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
