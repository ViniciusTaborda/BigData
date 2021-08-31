package advanced.customwritable;

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

import java.io.File;
import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        //registro das classes

        j.setJarByClass(AverageTemperature.class);

        j.setMapperClass(MapForAverage.class);

        j.setReducerClass(ReduceForAverage.class);



        //definicao do tipo de entrada e saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);
        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //Obter conteudo da linha
            String linha = value.toString();

            //Quebrando a linha em campos
            String campos[] = linha.split(",");

            //Acessar posição 8 que é a temperatura
            double temperatura = Double.parseDouble(campos[8]);
            double vento = Double.parseDouble(campos[10]);
            String mes = campos[2];

            //ococrrencia
            int ocorrencia = 1;

            //emitindo dados do map para o sort/shuffle e depois para o reduce
            //*Chave*
            //eu preciso de uma chave comum para todos os valores, pois assim garantimos que todos os valores
            // serão processados pelo reduce de forma conjunta
            con.write(new Text(mes), new FireAvgTempWritable(vento, temperatura) );


        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, Text> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            //Ideia do reduce:
            //Somar os ns
            //Somar as temperaturas
            double temp = 0;
            double wind = 0;

            double maxTemp = 0;
            double maxWind = 0;

            for (FireAvgTempWritable o : values) {
                temp = o.getTemp();
                wind = o.getWind();

                if (temp > maxTemp){
                    maxTemp = temp;
                }

                if (wind > maxWind){
                    maxWind = wind;
                }

            }

            String maxValues = maxWind + " " + maxTemp;

            //Escrever em arquivo
            con.write(new Text(key), new Text(maxValues));


        }
    }

}
