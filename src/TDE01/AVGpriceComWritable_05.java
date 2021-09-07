package TDE01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AVGpriceComWritable_05 implements WritableComparable {

    private double valor;
    private int n;

    public AVGpriceComWritable_05() {
    }

    public AVGpriceComWritable_05(int n, double valor) {
        this.n = n;
        this.valor = valor;
    }


    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getValor() {
        return n;
    }

    public void setValor(double valor) {
        this.valor = valor;
    }


    @Override
    public int hashCode() {
        return Objects.hash(n, valor);
    }

    @Override
    public int compareTo(Object o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n);
        dataOutput.writeDouble(valor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        valor=dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "AVGpriceCommodityWritable1{" +
                "valor=" + valor +
                ", n=" + n +
                '}';
    }
}
