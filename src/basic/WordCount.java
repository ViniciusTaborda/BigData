package basic;

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


public class WordCount {

    public static void main(String[] args) throws Exception {
        // Linhas de configuraÃ§ao sÃ£o de configuraÃ§Ã£o base do Hadoop e do log
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // Definicao dos arquivos de entrada e saida
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");
        // deprecated => vai deixar de funcionar em algum momento
        // JobConf

        // registro de classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        // definicao dos tipos de saida
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
        if(b) System.exit(0);
        System.exit(1);
    }

    /*
     * - A funcao de map vai ser chamada por LINHA do arquivo.
     * - Isso significa que eu NAO preciso fazer um laco de repeticao para tratar multiplas linhas
     * de um bloco de arquivo.
     *
     * Estrutura:
     *
     * Mapper<Tipo da chave de entrada, Tipo do valor de entrada, Tipo da chave de saida, Tipo de valor de saida>
     *
     * offset Ã© um marcador em relaÃ§ao ao inicio do arquivo
     * offset Ã© uma informaÃ§Ã£o que a gente nÃ£o usa
     * (offset, linha) -> map -> (palavra, ocorrencia)
     * (long, string) -> map -> (string, int)
     *
     * long => LongWritable
     * int => IntWritable
     * double => DoubleWritable
     * float => FloatWritable
     * string => Text
     */
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        /*
         * key: offset, irrelevante para nÃ³s
         * value: uma linha do arquivo a ser processado
         * con: responsÃ¡vel por comunicar o map com sort/shuffle e tb com o reduce
         *
         * - Nao esquecer de que os dois primeiros tipos do Mapper<> devem casar com os
         * tipos dos parametros da funcao abaixo:
         */
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // extrair o conteudo da linha
            String conteudo = value.toString();

            // 1:1 In the beginning God created the heaven and the earth.

            // quebrando a linha em palavras
            String[] palavras = conteudo.split(" ");

            //palavras = [1:1, In, the, beginning, God, created, the, heaven, and, the, earth.]

            // para cada palavra, gerando (palavra, 1) => (chave, valor)
            // (1:1, 1)
            // (In, 1)
            // (the, 1)
            // ...
            // (the, 1)
            // (earth., 1)

            for (String p : palavras){
                // gerando chave
                Text chaveSaida = new Text(p);
                // gerando valor
                IntWritable valorSaida = new IntWritable(1);
                // usando o contexto para passar cada (chave,valor) para o sort/shuffle
                con.write(chaveSaida, valorSaida);
            }
        }
    }

    /*
     * - Recebe como entrada (chave, lista de valores)
     * - Reduce Ã© chamado depois do sort/shuffle
     *
     * - Reducer<Tipo da chave de entrada, Tipo do valor de entrada, Tipo da chave de saida, Tipo do valor de saida>
     *
     * - Os dois primeiros tipos devem ser IDENTICOS aos tipos de saida do Map!
     */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        /*
         *
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // key eh minha palavra
            // values eh minha lista de ocorrencias (1,1,1,1,1,1,1,...)

            // somar para cada palavra (chave) a soma das ocorrencias
            int soma = 0;
            for (IntWritable v : values){
                soma += v.get(); // conversao para inteiro puro do java
                //soma++; // nao vai ser escalavel/otimizavel
            }

            // retornar (chave, valor) => (palavra, soma)
            con.write(key, new IntWritable(soma));

        }
    }

}