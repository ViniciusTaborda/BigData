package advanced.entropy;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BaseQtdWritable implements WritableComparable<BaseQtdWritable>{
    private String caracter;
    private long qtd;

    public BaseQtdWritable() {
    }

    public BaseQtdWritable(String caracter, long qtd) {
        this.caracter = caracter;
        this.qtd = qtd;
    }


    public String getCaracter() {
        return caracter;
    }

    public void setCaracter(String caracter) {
        this.caracter = caracter;
    }

    public long getQtd() {
        return qtd;
    }

    public void setQtd(long qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(BaseQtdWritable o) {
        if(this.hashCode() < o.hashCode())return -1;
        else if(this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(caracter);
        dataOutput.writeLong(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        caracter = dataInput.readUTF();
        qtd = dataInput.readLong();
    }

    @Override
    public int hashCode() {
        return Objects.hash(caracter, qtd);
    }
}