package es.sinbad2.stub;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {

    public static final String appName = "spark-java-stub";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName(appName);

        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("================ Example 1 ===================");
        example1(sc);
        System.out.println("==============================================\n\n\n\n");

        System.out.println("================ Example 2 ===================");
        example2(sc,"text.txt","text-output.txt");
        System.out.println("==============================================\n\n\n\n");

        System.out.println("================ Example 2.5 =================");
        example2_withStopWords(sc,"text.txt","text-output.txt","stopwords.txt");
        System.out.println("==============================================");

    }

    private static double example1(JavaSparkContext sc) {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> data_rdd = sc.parallelize(data);
        JavaRDD<Integer> data_rdd2 = data_rdd.map(value -> value * 2);
        double sum = data_rdd2.reduce((a,b) -> a+b );
        // sum = 30

        System.out.println("The sum elements is "+sum);
        return sum;
    }

    private static JavaPairRDD<String, Integer> example2(JavaSparkContext sc, String textFilePath, String outputPath) {
        JavaRDD<String> textFile = sc.textFile(textFilePath);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .map(s-> s.toLowerCase())
                .map(s-> s.replaceAll(",",""))
                .map(s-> s.replaceAll("\\.",""))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer,String> countsSortedByN = counts
                .mapToPair(tuple -> new Tuple2<>(tuple._2,tuple._1))
                .sortByKey(false);

        List<Tuple2<Integer, String>> countsByN_inDriver = countsSortedByN
                .collect();
        countsByN_inDriver.stream().forEach((Tuple2<Integer,String> tuple2)-> {
            System.out.println("("+tuple2._2+","+tuple2._1+")");
        });

        clearPath(outputPath);
        counts.saveAsTextFile(outputPath);

        return counts;
    }

    private static JavaPairRDD<String, Integer> example2_withStopWords(JavaSparkContext sc, String textFilePath, String outputPath, String stopwordsPath) {
        JavaRDD<String> textFile = sc.textFile(textFilePath);
        Set<String> stopwords = sc.textFile(stopwordsPath)
                .collect()
                .stream()
                .collect(Collectors.toSet());

        Broadcast<Set<String>> stopwordsBC = sc.broadcast(stopwords);

        JavaPairRDD<String, Integer> countsNoStopwords = textFile
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(word-> word.toLowerCase())
                .map(word-> word.replaceAll(",",""))
                .map(word-> word.replaceAll("\\.",""))
                .filter(word -> {
                    Set<String> stopwordsInWorker = stopwordsBC.getValue();
                    return !stopwordsInWorker.contains(word);
                })
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer,String> countsNoStopwordsSortedByN = countsNoStopwords
                .mapToPair(tuple -> new Tuple2<>(tuple._2,tuple._1))
                .sortByKey(false);

        List<Tuple2<Integer, String>> countsByN_inDriver = countsNoStopwordsSortedByN
                .collect();
        countsByN_inDriver.stream().forEach((Tuple2<Integer,String> tuple2)-> {
            System.out.println("("+tuple2._2+","+tuple2._1+")");
        });

        JavaPairRDD<String, Integer> countsStopwordsTo0 = textFile
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(word-> word.toLowerCase())
                .map(word-> word.replaceAll(",",""))
                .map(word-> word.replaceAll("\\.",""))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(tuple -> {
                    Set<String> stopwordsInWorker = stopwordsBC.getValue();
                    if(stopwordsInWorker.contains(tuple._1)){
                        return new Tuple2<>(tuple._1,0);
                    }else{
                        return tuple;
                    }
                });

        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .map(s-> s.toLowerCase())
                .map(s-> s.replaceAll(",",""))
                .map(s-> s.replaceAll("\\.",""))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        Map<String, Tuple2<Integer, Integer>> countsJoined = countsStopwordsTo0.join(counts)
                .sortByKey()
                .collectAsMap();

        System.out.println("word \t count w/o stopwords, count w/ stopwords");
        countsJoined.forEach((key,value) -> {
            System.out.println("("+key+", "+value._1+", "+value._2+")");
        });

        return countsStopwordsTo0;
    }

    private static void clearPath(File file) {
        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            Arrays.asList(file.listFiles()).forEach(Main::clearPath);
        }

        file.delete();
    }

    private static void clearPath(String outputPath) {
        File outputFile = new File(outputPath);
        clearPath(outputFile);
    }
}