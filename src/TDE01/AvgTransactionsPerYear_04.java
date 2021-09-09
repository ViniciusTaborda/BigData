package TDE01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AvgTransactionsPerYear {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "TransactionsAvg");

        // registro das classes
        j.setJarByClass(AvgTransactionsPerYear.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        // map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AvgTransactionsWritable.class);

        // reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, AvgTransactionsWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Obter o conteudo da linha
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String campos[] = linha.split(";");

            String ano = campos[1];
            double trade_usd = Double.parseDouble(campos[5]);
            con.write(new Text(ano),
                    new AvgTransactionsWritable(1, trade_usd));


        }
    }

    public static class ReduceForAverage extends Reducer<Text, AvgTransactionsWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<AvgTransactionsWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaValues = 0.0;
            int somaCs = 0;
            for (AvgTransactionsWritable o : values) {
                somaValues += o.getCommodity_value();
                somaCs += o.getC();
            }

            double media = somaValues / somaCs;

            con.write(new Text(key), new DoubleWritable(media));

        }
    }

}