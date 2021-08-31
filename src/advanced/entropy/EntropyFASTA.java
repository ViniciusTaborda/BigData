package advanced.entropy;

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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // Job da primeira etapa (contagem)

        Job j1 = new Job(c, "contagem");


        // registro de classes

        j1.setJarByClass(EntropyFASTA.class);

        j1.setMapperClass(MapEtapaA.class);

        j1.setReducerClass(ReduceEtapaA.class);


        // definicao dos tipos de saida

        // map

        j1.setMapOutputKeyClass(Text.class);

        j1.setMapOutputValueClass(LongWritable.class);


        //reduce

        j1.setOutputKeyClass(Text.class);

        j1.setOutputValueClass(LongWritable.class);


        // cadastro dos arquivos de entrada e saida

        FileInputFormat.addInputPath(j1, input);

        FileOutputFormat.setOutputPath(j1, intermediate);


        if (!j1.waitForCompletion(true)) {
            System.err.println("Erro no job1!");
            return;
        }

        //Depois que o job1 rodou repetimos para um novo job

        //--------------------------------------------------------------

        // Depois que o job 1 rodou, repetimos o processo para um novo job (entropia)

        Job j2 = new Job(c, "entropia");


        // registro de classes

        j2.setJarByClass(EntropyFASTA.class);

        j2.setMapperClass(MapEtapaB.class);

        j2.setReducerClass(ReduceEtapaB.class);


        // definicao de tipos de saida

        //map

        j2.setMapOutputKeyClass(Text.class);

        j2.setMapOutputValueClass(BaseQtdWritable.class);


        //reduce

        j2.setOutputKeyClass(Text.class);

        j2.setOutputValueClass(DoubleWritable.class);


        // cadastro de arquivos de entrada e saida

        FileInputFormat.addInputPath(j2, intermediate);

        FileOutputFormat.setOutputPath(j2, output);


        // executar o job

        j2.waitForCompletion(true);

    }


    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // receber uma linha pra processar
            String linha = value.toString();

            //ignora o cabeçalho
            if (linha.startsWith(">")) return;
            //linha com conteudo mesmo (caracteres)
            String[] caracteres = linha.split("");

            // laco de repetição para gerar as ocorrencias (c, 1)
            for (String c : caracteres) {
                con.write(new Text(c), new LongWritable(1));
                con.write(new Text("Total"), new LongWritable(1));

            }


        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            //Reduce é chamado uma vez por chave e pode ser cada caractere ou total
            //somar as ocorrencias por chave
            long qtd = 0;
            for (LongWritable v : values) {
                qtd += v.get();
            }
            //resultado final, gera  caracter e quantidade
            con.write(key, new LongWritable(qtd));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //ler o arquivo intermediario linha a linha
            String linha = value.toString();

            String[] campos = linha.split("\t");
            //caracter
            String c = campos[0];
            //qtd
            long qtd = Long.parseLong(campos[1]);

            //repassar essa informacao para o reduce mas com uma chave unica
            con.write(new Text("entropia"), new BaseQtdWritable(c, qtd));


        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {
            // reduce vai receber ("entropia", [(A, 234), (B, 344) ... (total, 1000)])
            long total = 0;
            //buscando o valor total
            for (BaseQtdWritable o : values) {
                if (o.getCaracter().equals("Total")) {
                    total = o.getQtd();
                    break;
                }
            }

            //percorrer a lista de novo calculando a probabilidade e a entropia para os caracteres

            for (BaseQtdWritable o : values) {
                //se o caracter nao for igual o total eu calculo entropia pra ele
                if (!o.getCaracter().equals("Total")) {
                    double p = o.getQtd() / (double) total;

                    double entropia = -p * Math.log10(p) / Math.log10(2.0);

                    //gerando resultados
                    con.write(new Text(o.getCaracter()), new DoubleWritable(entropia));
                }
            }
        }
    }

}
