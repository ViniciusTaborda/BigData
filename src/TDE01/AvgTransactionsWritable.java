package TDE01;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgTransactionsWritable implements WritableComparable<AvgTransactionsWritable> {

    private int c;
    private double commodity_value;

    public AvgTransactionsWritable() {
    }

    public AvgTransactionsWritable(int c, double commodity_value) {
        this.c = c;
        this.commodity_value = commodity_value;
    }

    public int getC() {
        return c;
    }

    public void setC(int c) {
        this.c = c;
    }

    public double getCommodity_value() {
        return commodity_value;
    }

    public void setCommodity_value(double commodity_value) {
        this.commodity_value = commodity_value;
    }

    @Override
    public int compareTo(AvgTransactionsWritable o) {
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
        return Objects.hash(c, commodity_value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(c);
        dataOutput.writeDouble(commodity_value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        c = dataInput.readInt();
        commodity_value = dataInput.readDouble();
    }

}
