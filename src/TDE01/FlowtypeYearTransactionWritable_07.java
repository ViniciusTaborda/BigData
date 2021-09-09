package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowtypeYearTransactionWritable implements WritableComparable<FlowtypeYearTransactionWritable> {

    private int year;
    private String flow_type;

    public FlowtypeYearTransactionWritable() {
    }

    public FlowtypeYearTransactionWritable(int year, String flow_type) {
        this.year = year;
        this.flow_type = flow_type;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getFlow_type() {
        return flow_type;
    }

    public void setFlow_type(String flow_type) {
        this.flow_type = flow_type;
    }

    @Override
    public int compareTo(FlowtypeYearTransactionWritable o) {
        if (this.hashCode() < o.hashCode()) {
            return -1;
        } else if (this.hashCode() > o.hashCode()) {
            return +1;
        } else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, flow_type);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeUTF(flow_type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        flow_type = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "{" +
                "flow_type=" + this.flow_type +
                ", year=" + this.year +
                '}';
    }


}
