package edu.gslis.hadoop.wiki;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration;

import edu.gslis.hadoop.wiki.XmlInputFormat.XmlRecordReader;

public class WikiPageInputFormat extends
    FileInputFormat<LongWritable, WikiPage> {

	@Override
	public RecordReader<LongWritable, WikiPage> getRecordReader(
			InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		return new WikiDocumentRecordReader((FileSplit)split, job);
	}	
	

  public static class WikiDocumentRecordReader implements
      RecordReader<LongWritable, WikiPage> 
  {
	  private XmlRecordReader reader = null;
	  
	  //SimpleWikiConfiguration config = new SimpleWikiConfiguration(
	  //            "classpath:/org/sweble/wikitext/engine/SimpleWikiConfiguration.xml");

	  // Instantiate a compiler for wiki pages
	  //org.sweble.wikitext.engine.Compiler compiler = new org.sweble.wikitext.engine.Compiler(config);
	  
	  public WikiDocumentRecordReader(FileSplit split, JobConf conf) throws IOException
	  {
	      conf.set(XmlInputFormat.START_TAG_KEY, WikiPage.XML_START_TAG);
	      conf.set(XmlInputFormat.END_TAG_KEY, WikiPage.XML_END_TAG);
		  reader = new XmlRecordReader(split, conf);
	  }
    
	  public boolean next(LongWritable key, WikiPage doc) throws IOException {
		  Text text = new Text();
		  boolean ret = reader.next(key, text);
		  WikiPage.readDocument(doc, text.toString());
		  return ret;
	  }
	  public LongWritable createKey() {
		return reader.createKey();
	  }
	  
	  public WikiPage createValue() {
	      return new WikiPage();
		  //return new WikiPage(compiler, config);
	  }
	  public long getPos() throws IOException {
		return reader.getPos();
	  }
	  public void close() throws IOException {
		reader.close();
	  }
	
	  public float getProgress() throws IOException {
		return reader.getProgress();
	  }
   }
}
