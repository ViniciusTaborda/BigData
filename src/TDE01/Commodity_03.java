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

public class Commodity_03 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "commodity2016");

        j.setJarByClass(Commodity_03.class);
        j.setMapperClass(MapForTransactionsCount.class);
        j.setReducerClass(ReduceForTransactionsCount.class);

        // map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        // reduce
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // executar
        boolean b = j.waitForCompletion(true);
        if (b) System.exit(0);
        System.exit(1);
    }

    public static class MapForTransactionsCount extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] dados = linha.split(";");

            String ano = dados[1];
            String flow = dados[4];

            double qtd = Double.parseDouble(dados[8]);
            //double qtd = 1.0;

            int ocorrencia = 1;

            if (ano.equals("2016")) {
                //con.write(new Text(flow), new IntWritable(1));
                con.write(new Text(flow), new DoubleWritable(qtd));

            }
        }
    }

    public static class ReduceForTransactionsCount extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            for (DoubleWritable v : values) {
                soma += v.get();

            }
            con.write(key, new DoubleWritable(soma));

        }
    }


}
