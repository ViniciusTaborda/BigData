package TDE01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AVGpriceCommodity_05 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "commodity2016");

        j.setJarByClass(AVGpriceCommodity_05.class);
        j.setMapperClass(MapForMediaPertype.class);
        j.setReducerClass(ReduceForMediaPertype.class);

        // map
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AVGpriceComWritable_05.class);
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

    public static class MapForMediaPertype extends Mapper<LongWritable, Text, Text, AVGpriceComWritable_05> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            if (linha.startsWith("country_or_area")) return;
            String[] dados = linha.split(";");


            double valorUSD = Double.parseDouble(dados[5]);
            String unitType = dados[7];
            String categoria = dados[9];
            String fluxo = dados[4];
            String ano = dados[1];
            String pais = dados[0];
            String chaveUCA = dados[7] + " " + dados[9] + " " +  dados[1];

            int ocorrencia = 1;

            if (pais.equals("Brazil") && (fluxo.equals("Export"))) {
                con.write(new Text(chaveUCA), new AVGpriceComWritable_05(ocorrencia, valorUSD));

            }

        }
    }

    public static class ReduceForMediaPertype extends Reducer<Text, AVGpriceComWritable_05, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<AVGpriceComWritable_05> values, Context con)
                throws IOException, InterruptedException {

            double somaValor = 0.0;
            int somaOc = 0;
            for (AVGpriceComWritable_05 v : values) {
                somaValor += v.getValor();
                somaOc += v.getN();
            }
            double media = somaValor / somaOc;
            con.write(key, new DoubleWritable(somaValor));

        }
    }




}
