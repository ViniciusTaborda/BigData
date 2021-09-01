package TDE01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionsPerFlowTypeYear {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "NumberOfTransactionsPerFlowType");

        // registro das classes
        j.setJarByClass(TransactionsPerFlowTypeYear.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        // map
        j.setMapOutputKeyClass(FlowtypeYearTransactionWritable.class);
        j.setMapOutputValueClass(IntWritable.class);

        // reduce
        j.setOutputKeyClass(FlowtypeYearTransactionWritable.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, FlowtypeYearTransactionWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Obter o conteudo da linha
            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String campos[] = linha.split(";");

            int ano = Integer.parseInt(campos[1]);
            String flow_type = campos[4];
            con.write(new FlowtypeYearTransactionWritable(ano, flow_type), new IntWritable(1));


        }
    }

    public static class ReduceForAverage extends Reducer<FlowtypeYearTransactionWritable, IntWritable, FlowtypeYearTransactionWritable, IntWritable> {
        public void reduce(FlowtypeYearTransactionWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;
            for (IntWritable o : values) {
                soma += o.get();
            }

            con.write(key, new IntWritable(soma));

        }
    }

}