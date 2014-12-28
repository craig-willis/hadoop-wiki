package edu.gslis.hadoop.wiki;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.sweble.wikitext.engine.CompiledPage;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration;

import com.google.common.base.Preconditions;

public class WikiPage implements Writable {

  public static final String XML_START_TAG = "<page>";
  public static final String XML_END_TAG = "</page>";

  private String content = null;
  private String title = null;
  private String text = null;  
  private String wikitext = null;
  private org.sweble.wikitext.engine.Compiler compiler;
  SimpleWikiConfiguration config;
  
  public WikiPage(org.sweble.wikitext.engine.Compiler compiler, SimpleWikiConfiguration config) {
      this.compiler = compiler;
      this.config = config;
  }

  public void write(DataOutput out) throws IOException {
    byte[] bytes = content.getBytes();
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  public void readFields(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    WikiPage.readDocument(this, new String(bytes));
  }

  public String getTitle() {
    if (title == null) {
      int start = content.indexOf("<title>");

      if (start == -1) {
          title = "";
      } else {
        int end = content.indexOf("</title>", start);
        title = content.substring(start + 7, end).trim();
        title = StringEscapeUtils.unescapeXml(title);
      }
    }

    return title;
  }

  public String getContent() {
    return content;
  }
  
  public String getWikiText() {
   if (wikitext == null) {
      int start = content.indexOf("<text xml:space=\"preserve\">");

      if (start == -1) {
          wikitext = "";
      } else {
        int end = content.indexOf("</text>", start);
        wikitext = content.substring(start + 28, end).trim();
        wikitext = StringEscapeUtils.unescapeXml(wikitext);
      }
    }

    return wikitext;
  }

  public String getText()
  {
      String title = getTitle();
      String wikiText = getWikiText();
      
      if (text == null) 
      {
          try
          {
              // Retrieve a page
              PageTitle pageTitle = PageTitle.make(config, title);
        
              PageId pageId = new PageId(pageTitle, -1);
        
              // Compile the retrieved page
              CompiledPage cp = compiler.postprocess(pageId, wikiText, null);
                
              TextConverter p = new TextConverter(config, 80);
              text = (String) p.go(cp.getPage());
    
          } catch (Exception e) {
              e.printStackTrace();
          }
      }
      return text;

  }
  
  /**
   * Reads a raw XML string into a {@code WikiDocument} object.
   *
   * @param doc the {@code WikiDocument} object
   * @param s raw XML string
   */
  public static void readDocument(WikiPage doc, String s) {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(doc);

    doc.content = s;
    doc.title = null;
    doc.text = null;
    doc.wikitext = null;
  }
}