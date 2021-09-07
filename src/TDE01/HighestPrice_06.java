package TDE01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class HighestPrice_06 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "maiorPreco");

        // registro de classes
        j.setJarByClass(HighestPrice_06.class);
        j.setMapperClass(ForestFireMapper.class);
        j.setReducerClass(ForestFireReducer.class);

        // definicao dos tipos de saida
        // map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);

        // reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // executa o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class ForestFireMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] campos = linha.split(";");

            String chaveTipoAno = campos[7] + " " + campos[1] ;

            double preco = Double.parseDouble(campos[5]);

            context.write(new Text(chaveTipoAno), new DoubleWritable(preco));
        }
    }

    public static class ForestFireReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key,
                           Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {

            double maxPrice = Double.MIN_VALUE;

            for(DoubleWritable o : values){
                if (o.get() > maxPrice) maxPrice = o.get();

            }

            // escrevendo os maiores valores em arquivo
            context.write(key, new DoubleWritable(maxPrice));

        }
    }



}
