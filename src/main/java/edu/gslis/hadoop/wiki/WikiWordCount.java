    package edu.gslis.hadoop.wiki;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lemurproject.kstem.KrovetzStemmer;
import org.lemurproject.kstem.Stemmer;



/**
 * Count word frequencies in a collection of Wiki text.
 * Assumes a single input file of XML-formatted Wikipedia page data.
 * Stems terms using a Krovetz stemmer -- ignores numbers. 
 */
public class WikiWordCount extends Configured implements Tool 
{

    static int MIN_FREQ = 1;

    static int WIN_SIZE = 10;
    
    public static class WikiWordCountMapper extends MapReduceBase 
        implements Mapper<LongWritable, WikiPage, Text, IntWritable> 
    {

      public static enum Count { DOCS, WORDS  };

      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      private long numRecords = 0;
      private String inputFile;
      
      Stemmer stemmer = new KrovetzStemmer();


      public void map(LongWritable key, WikiPage value, OutputCollector<Text, IntWritable> output, 
              Reporter reporter) throws IOException 
      {
          if (value == null || value.getText() == null) 
              return;

          reporter.incrCounter(Count.DOCS, 1);
  
          String line = value.getText();
        
          line = line.replaceAll("[^A-Za-z0-9]", " ");
          line = line.toLowerCase();
        
          // Stem
          StringTokenizer tokenizer = new StringTokenizer(line);
          List<String> stemmed = new ArrayList<String>();
          while (tokenizer.hasMoreTokens()) {
              String w = tokenizer.nextToken();
              if (! w.matches("[0-9]+")) {
                  stemmed.add(stemmer.stem(w));
              }
          }
          
          for (int i=0; i < (stemmed.size() - WikiWordCount.WIN_SIZE) ; i++) {
              List<String> window = stemmed.subList(i, i + WIN_SIZE);
              Set<String> pseudoDoc = new HashSet<String>();
              pseudoDoc.addAll(window);
              
              for (String w: pseudoDoc) {
                  word.set(w);
                  output.collect(word, one);
                  reporter.incrCounter(Count.WORDS, 1);
              }
          }
          
          

          if ((++numRecords % 100) == 0) {
              reporter.setStatus("Finished processing " + numRecords + " records " 
                    + "from the input file: " + inputFile);
          }
       }
    }

    public static class WikiWordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    
        public void reduce(Text key, Iterator<IntWritable> values, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
        {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            
            if (sum >= MIN_FREQ)
                output.collect(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception 
    {
        JobConf conf = new JobConf(getConf(), WikiWordCount.class);
        conf.setJobName("wiki-wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WikiWordCountMapper.class);
        conf.setCombinerClass(WikiWordCountReducer.class);
        conf.setReducerClass(WikiWordCountReducer.class);

        conf.setInputFormat(WikiPageInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikiWordCount(), args);
        System.exit(res);
    }
}

