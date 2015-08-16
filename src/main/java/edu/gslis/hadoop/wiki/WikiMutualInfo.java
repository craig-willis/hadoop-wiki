package edu.gslis.hadoop.wiki;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.lemurproject.kstem.KrovetzStemmer;
import org.lemurproject.kstem.Stemmer;

import edu.gslis.hadoop.wiki.WikiWordCount.WikiWordCountMapper;
import edu.gslis.hadoop.wiki.WikiWordCount.WikiWordCountReducer;



public class WikiMutualInfo extends Configured implements Tool 
{
    private static final Logger logger = Logger.getLogger(WikiMutualInfo.class);

    public static class WikiMutualInfoMapper extends MapReduceBase implements Mapper<LongWritable, WikiPage, 
        Text, MapWritable> {

      private final static IntWritable one = new IntWritable(1);
      Text term1 = new Text();
      
      Stemmer stemmer = new KrovetzStemmer();
      Pattern numberMatcher = Pattern.compile("\\d+(\\.\\d+)?");
      Set<String> wordList = new HashSet<String>();

      public void configure (JobConf conf) 
      {
          try 
          {
              // Read the DocumentWordCount output file
              URI[] files = DistributedCache.getCacheFiles(conf);
              if (files != null) {
                  for (URI file: files) {
                      if (file.toString().contains("mutual-info-words"))
                      {
                          
                          FileSystem fs = FileSystem.get(new Configuration());
                          Path path = new Path(file.toString());
                          BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
                          String line;
                          while ((line = br.readLine()) != null) {
                              wordList.add(stemmer.stem(line));
                          }
                          
                          System.out.println("Read " + wordList.size() + " words from wordList " + file.toString());
                      }
                  }
              }
              else {
                  logger.error("Can't load cache files. Trying local cache");
                  Path[] paths = DistributedCache.getLocalCacheFiles(conf);
                  for (Path path: paths) {
                      if (path.toString().contains("mutual-info-words"))
                      {
                          List<String> words = FileUtils.readLines(new File(path.toString()));
                          wordList.addAll(words);
                          System.out.println("Read " + wordList.size() + " words from wordList " + path.toString());
                      }
                  }
              }
          } catch (Exception ioe) {
              ioe.printStackTrace();
              logger.error(ioe);
          }            
          
          
      }      


      public void map(LongWritable key, WikiPage value, OutputCollector<Text, MapWritable> output, 
              Reporter reporter) throws IOException 
      {
          if (value == null || value.getText() == null) 
              return;
          
          String line = value.getText();
          
          line = line.replaceAll("[^A-Za-z0-9]", " ");
          line = line.toLowerCase();
        
          StringTokenizer tokenizer = new StringTokenizer(line);
          List<String> stemmed = new ArrayList<String>();
          while (tokenizer.hasMoreTokens()) {
              String w = tokenizer.nextToken();
              if (! w.matches("[0-9]+")) {
                  stemmed.add(stemmer.stem(w));
              }
          }
          for (int i=0; i < (stemmed.size() - WikiWordCount.WIN_SIZE) ; i++) {
              List<String> window = stemmed.subList(i, i + WikiWordCount.WIN_SIZE);
              Set<String> words = new HashSet<String>();
              words.addAll(window);
              
              Iterator<String> it1 = words.iterator();
              while (it1.hasNext()) {
                  // Create an associative array containing all 
                  // co-occurring words.  Note: this is symmetric, 
                  // but shouldn't effect MI values.
                  String word1 = (String)it1.next();
                  
                  // If provided, only collect terms for those words in the list
                  if ((wordList.size() > 0) && (!wordList.contains(word1)))
                      continue;
                  
                  MapWritable map = new MapWritable();
                  term1.set(word1);
                  
                  Iterator<String> it2 = words.iterator();
                  while (it2.hasNext()) {
                      String word2 = (String)it2.next();
                      
                      if (word1.equals(word2)  || (wordList.size() > 0) && (!wordList.contains(word2)))
                          continue;
                      
                      Text term2 = new Text();
    
                      term2.set(word2);
                      map.put(term2, one);
                  }
                  output.collect(term1, map);
              }
          }          
       }
    }

    public static class WikiMutualInfoReducer extends MapReduceBase 
        implements Reducer<Text, MapWritable, Text, DoubleWritable> 
    {
    
        Map<String, Integer> documentFreq = new HashMap<String, Integer>();
        Text wordPair = new Text();
        DoubleWritable mutualInfo = new DoubleWritable();
        int totalNumDocs = 0;

        /**
         * Side-load the output from word count job
         */
        public void configure(JobConf conf) 
        {
            logger.info("Setup");
            
            totalNumDocs = Integer.parseInt(conf.get("numDocs"));
            logger.info("TOTAL_NUM_DOCS=" + totalNumDocs);

            try {
                // Read the DocumentWordCount output file
                URI[] files = DistributedCache.getCacheFiles(conf);
                if (files != null) {
                    for (URI file: files) {
                        if (file.toString().contains("mutual-info-words"))
                            continue;
                        logger.info("Reading total word counts from: " + file.toString());
                        
                        FileSystem fs = FileSystem.get(new Configuration());
                        Path path = new Path(file.toString());
                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] fields = line.split("\t");
                            documentFreq.put(fields[0], Integer.parseInt(fields[1]));
                        }
                        logger.info("Read " + documentFreq.size() + " words");
                    }
                }
                else 
                {
                    logger.error("Can't load cache files. Trying local cache");
                    Path[] paths = DistributedCache.getLocalCacheFiles(conf);
                    if (paths != null) {                        
                        for (Path path: paths) {
                            if (path.toString().contains("mutual-info-words"))
                                continue;
                            logger.info("Reading total word counts from: " + path.toString());
                            List<String> lines = FileUtils.readLines(new File(path.toUri().toString()));
                            for (String line: lines) {
                                String[] fields = line.split("\t");
                                documentFreq.put(fields[0], Integer.parseInt(fields[1]));
                            }
                            logger.info("Read " + documentFreq.size() + " words");
                        }
                    }
                }
            } catch (Exception ioe) {
                ioe.printStackTrace();
                logger.error(ioe);
            }
        }

        
        public void reduce(Text term, Iterator<MapWritable> values, 
                OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException 
        {            
            // key contains a given word and values contains a set of
            // associative arrays containing all co-occurring words.  Each
            // value represents all co-occurring words in a single document.
            // Collect all of the co-occurrences into a single map.
            Map<String, Integer> jointOccurrences = new HashMap<String, Integer>();
            while (values.hasNext())
            {
                MapWritable map = values.next();
                Set<Writable> keys = map.keySet();
                for (Writable key: keys)
                {
                    IntWritable count = (IntWritable)map.get(key);
                    String word2 = key.toString();
                    
                    if (jointOccurrences.containsKey(word2)) {
                        int sum = jointOccurrences.get(word2);
                        sum += count.get();
                        jointOccurrences.put(word2, sum);
                    } else {
                        jointOccurrences.put(word2, count.get());
                    }
                }
            }
    
            // For each word pair, calculate EMIM.
            String word1 = term.toString();
            for (String word2: jointOccurrences.keySet()) 
            {
                if (documentFreq.containsKey(word1) && 
                        documentFreq.containsKey(word2))
                {
                    //        | wordY | ~wordY |
                    // -------|-------|--------|------
                    //  wordX | nX1Y1 | nX1Y0  | nX1
                    // ~wordX | nX0Y1 | nX0Y0  | nX0
                    // -------|-------|--------|------
                    //        |  nY1  |  nY0   | total
    
                    double nX1Y1 = jointOccurrences.get(word2);
                    double nX1 = documentFreq.get(word1);
                    double nY1 = documentFreq.get(word2);
    
                    //logger.info(word1 + "," + word2 + "," + totalNumDocs + "," + nX1Y1 + "," + nX1 + "," + nY1);
                    //double emim = calculateEmim(totalNumDocs, nX1Y1, nX1, nY1);
                    double npmi = calculateNPMI(totalNumDocs, nX1Y1, nX1, nY1);
                    
                    
                    wordPair.set(word1 + "\t" + word2);
                    mutualInfo.set(npmi);
                    output.collect(wordPair, mutualInfo);
                }
            }
        }
        
        /**
         * The actual mutual information calculation.  Given the total
         * number of terms (N), the joint occurrences of word1 and word2,
         * and the marginals of word1 and word2.
         */
        private double calculateEmim(double N, double nX1Y1, double nX1, double nY1)
        {
            
            //        | wordY | ~wordY |
            // -------|-------|--------|------
            //  wordX | nX1Y1 | nX1Y0  | nX1
            // ~wordX | nX0Y1 | nX0Y0  | nX0
            // -------|-------|--------|------
            //        |  nY1  |  nY0   | gt
    
            // Marginal and joint frequencies
            double nX0 = N - nX1;
            double nY0 = N - nY1;       
            double nX1Y0 = nX1 - nX1Y1;
            double nX0Y1 = nY1 - nX1Y1;
            double nX0Y0 = nX0 - nX0Y1;
    
            // Marginal probabilities (smoothed)
            double pX1 = (nX1 + 0.5)/(1+N);
            double pX0 = (nX0 + 0.5)/(1+N);         
            double pY1 = (nY1 + 0.5)/(1+N);
            double pY0 = (nY0 + 0.5)/(1+N);
            
            // Joint probabilities (smoothed)
            double pX1Y1 = (nX1Y1 + 0.25) / (1+N);
            double pX1Y0 = (nX1Y0 + 0.25) / (1+N);
            double pX0Y1 = (nX0Y1 + 0.25) / (1+N);
            double pX0Y0 = (nX0Y0 + 0.25) / (1+N);
            
            // 
            double emim =  
                    pX1Y1 * log2(pX1Y1, pX1*pY1) + 
                    pX1Y0 * log2(pX1Y0, pX1*pY0) +
                    pX0Y1 * log2(pX0Y1, pX0*pY1) +
                    pX0Y0 * log2(pX0Y0, pX0*pY0);
            
            return emim;
        }
        
        private double calculateNPMI(double N, double nX1Y1, double nX1, double nY1)
        {
            
            //        | wordY | ~wordY |
            // -------|-------|--------|------
            //  wordX | nX1Y1 | nX1Y0  | nX1
            // ~wordX | nX0Y1 | nX0Y0  | nX0
            // -------|-------|--------|------
            //        |  nY1  |  nY0   | gt
    
    
            // Marginal probabilities (smoothed)
            double pX1 = (nX1 + 0.5)/(1+N);
            double pY1 = (nY1 + 0.5)/(1+N);
            
            // Joint probabilities (smoothed)
            double pX1Y1 = (nX1Y1 + 0.25) / (1+N);
            
            // Ala http://www.aclweb.org/anthology/W13-0102
            double pmi = log2(pX1Y1, pX1*pY1);
            double npmi = pmi / Math.log(pX1Y1)/Math.log(2);
            
            return npmi;
        }

    }

    private static double log2(double num, double denom) {
        if (num == 0 || denom == 0)
            return 0;
        else
            return Math.log(num/denom)/Math.log(2);
    }
    

    public int run(String[] args) throws Exception 
    {
        Path inputPath = new Path(args[0]);
        Path wcOutputPath = new Path(args[1]);
        Path miOutputPath = new Path(args[2]);
        Path wordListPath = new Path(args[3]);
        String numDocsStr = null;
        if (args.length == 5)    
            numDocsStr = args[4];

        int numDocs = 0;
        if (numDocsStr == null) {
            JobConf wc = new JobConf(getConf(), WikiWordCount.class);
            wc.setJobName("wiki-wordcount");
    
            wc.setOutputKeyClass(Text.class);
            wc.setOutputValueClass(IntWritable.class);
    
            wc.setMapperClass(WikiWordCountMapper.class);
            wc.setReducerClass(WikiWordCountReducer.class);
    
            wc.setInputFormat(WikiPageInputFormat.class);
            wc.setOutputFormat(TextOutputFormat.class);
    
            FileInputFormat.setInputPaths(wc, inputPath);
            FileOutputFormat.setOutputPath(wc, wcOutputPath);
    
            RunningJob job = JobClient.runJob(wc);
           
            job.waitForCompletion();
    
            Counters counters = job.getCounters();
            numDocs = (int) counters.findCounter(WikiWordCountMapper.Count.DOCS).getValue();
        }
        else
            numDocs = Integer.parseInt(numDocsStr);
        
        
        JobConf mi = new JobConf(getConf(), WikiMutualInfo.class);
        mi.setJobName("wiki-pmi");
        mi.set("numDocs", String.valueOf(numDocs));

        mi.set("wordListPath", wordListPath.toUri().toString());
        DistributedCache.addCacheFile(wordListPath.toUri(), mi);

        mi.setJarByClass(WikiMutualInfo.class);

        mi.setMapperClass(WikiMutualInfoMapper.class);
        mi.setReducerClass(WikiMutualInfoReducer.class);

        mi.setInputFormat(WikiPageInputFormat.class);
        mi.setOutputFormat(TextOutputFormat.class);
                
        mi.setMapOutputKeyClass(Text.class);
        mi.setMapOutputValueClass(MapWritable.class);
        
        mi.setOutputKeyClass(Text.class);
        mi.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.setInputPaths(mi, inputPath);
        FileOutputFormat.setOutputPath(mi, miOutputPath);
                     
        FileSystem fs = FileSystem.get(mi);
        logger.info("Reading output from " + wcOutputPath.toUri().toString());
        Path pathPattern = new Path(wcOutputPath, "part-[0-9]*");
        FileStatus [] list = fs.globStatus(pathPattern);
        for (FileStatus status: list) {
            String name = status.getPath().toString();
            logger.info("Adding cache file " + name);
            DistributedCache.addCacheFile(new Path(wcOutputPath, name).toUri(), mi); 
        }
        
        JobClient.runJob(mi);
        
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikiMutualInfo(), args);
        System.exit(res);
    }
}

