package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

public class CommodidtWritable_03 implements WritableComparable {
    private int n;
    private double qtd;

    public CommodidtWritable_03(int ocorrencia, BigInteger qtd2) {
    }

    public CommodidtWritable_03(int n, double qtd) {
        this.n = n;
        this.qtd = qtd;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getQtd() {
        return qtd;
    }

    public void setQtd(long qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(Object o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n); //enviando a ocorrencia do objeto
        dataOutput.writeDouble(qtd); // enviado a temperatudo do objeto

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        qtd = dataInput.readDouble();

    }

    @Override
    public int hashCode() {
        return Objects.hash(n, qtd);
    }
}
