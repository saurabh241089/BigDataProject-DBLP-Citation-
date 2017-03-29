import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class PageRankMapper extends
    Mapper<LongWritable, Text, IntWritable, IntWritable> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	   ///Replacing all digits and punctuation with an empty string
		  String line = value.toString();
	   //Extracting the words
		  StringTokenizer record = new StringTokenizer(line, "\n");
	   //Emitting each word as a key and one as its value
		  int index = 0, citedIndex = 0;
		  while (record.hasMoreTokens()){
			  
			  String token = record.nextToken();
			  if(token.contains("index")){
				  try{
					  index = Integer.parseInt(token.substring(token.indexOf("x")+1).trim());
				  }catch(NumberFormatException exp){					  
					  index = -1;
				  }
				  
			  }else if(token.contains("%")){
				  try{
					  citedIndex = Integer.parseInt(token.substring(token.indexOf("%")+1).trim());
				  }catch(NumberFormatException exp){
					  citedIndex = -1;
				  }
				  if(index != -1 && citedIndex != -1)
				  context.write(new IntWritable(index), new IntWritable(citedIndex));
			  }
			  
		  }
		    
    }
  }

