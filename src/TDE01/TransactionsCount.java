package TDE01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


public class TransactionsCount {

    public static void main(String[] args) throws Exception {
        // Linhas de configuracao sao de configuracao base do Hadoop e do log
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // Definicao dos arquivos de entrada e saida
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "transactionsCount");

        // registro de classes
        j.setJarByClass(TransactionsCount.class);
        j.setMapperClass(MapForTransactionsCount.class);
        j.setReducerClass(ReduceForTransactionsCount.class);

        // map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        // reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // executar
        boolean b = j.waitForCompletion(true);
        if (b) System.exit(0);
        System.exit(1);
    }

    public static class MapForTransactionsCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // extrair o conteudo da linha
            String linha = value.toString();

            if (linha.startsWith("country_or_area")) return;
            String[] dados = linha.split(";");
            String pais = dados[0];


            if (pais.equals("Brazil")) {
                //mandando pais e a ocorrencia 1 para o reduce
                con.write(new Text(pais), new IntWritable(1));
            }

        }
    }

    public static class ReduceForTransactionsCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            con.write(key, new IntWritable(soma));

        }
    }

}