package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

//Todo writable precisa se rum java bean
//Java bean
public class FireAvgTempWritable implements WritableComparable<FireAvgTempWritable> {

    private double wind;
    private double temp;

    public FireAvgTempWritable() {
    }

    public FireAvgTempWritable(double wind, double temp) {
        this.wind = wind;
        this.temp = temp;
    }

    public double getWind() {
        return wind;
    }

    public void setWind(double wind) {
        this.wind = wind;
    }

    public double getTemp() {
        return temp;
    }

    public void setTemp(double temp) {
        this.temp = temp;
    }

    @Override
    public int compareTo(FireAvgTempWritable o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }else{
            return 0;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(wind, temp);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(wind); // enviando a ocorrencia do objeto
        dataOutput.writeDouble(temp); // enviando a temperatura do objeto
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        wind = dataInput.readDouble();
        temp = dataInput.readDouble();
    }
}
