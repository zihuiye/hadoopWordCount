/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;  

public class Edge {

  public static class SplitMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text v = new Text();
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

					
		String [] l = value.toString().split("[^0-9]");
		word.set(l[0]);
		v.set(l[1]);
		context.write(word,v);
		
		/*
		for(String line: l){
			if(!line.isEmpty()){
				line = line.toLowerCase();
				if(line.equals("harry")||line.equals("hermione")){
					
					word.set(line);
					context.write(word, one);
				}
				
			}
			
			
		}
		*/
	  /*
      StringTokenizer itr = new StringTokenizer(value.toString(),"[^a-zA-Z]");
      while (itr.hasMoreTokens()) {
		String s = itr.nextToken();
		s=s.toLowerCase();
		if(s.equals("harry")||s.equals("hermione")){
			word.set(s);
		}
		
        //word.set(itr.nextToken());
        
      }
	  */
	  
    }
  }
  public static class DuplicateCombiner extends Reducer<Text,Text,Text,Text>{
		private Text v = new Text();
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			
			
			HashSet<String> hs = new HashSet<String>();
			for(Text t:values){
				if(!hs.contains(t.toString())){
					hs.add(t.toString());
				}
				//context.write(key,t);
			}
			
			Iterator<String> itr = hs.iterator();
			while(itr.hasNext()){
				v.set(itr.next());
				context.write(key,v);
			}
			
		}
  }
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
	
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		
		
		HashSet<String> hs = new HashSet<String>();
		for(Text t:values){
			if(!hs.contains(t.toString())){
				hs.add(t.toString());
			}
			
			//context.write(key,t);
		}
		
		
		result.set(String.format("%02d",hs.size()));
		context.write(key,result);
		
    }
  }
  
  /*
  public static class exchangeMapper extends Mapper<Object, Text, Text, Text>{
	  
  }
	*/
	
	public class decentComparator implements RawComparator<Text> {
	
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1,s1,l1,b2,s2,l2);
		}
	
		 
		public int compare(WritableComparable a, WritableComparable b) {  
			
			return -super.compare(a,b);
		}
} 
	
	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(Edge.class);
    job.setMapperClass(SplitMapper.class);
    job.setCombinerClass(DuplicateCombiner.class);
	
	//job.setSortComparatorClass(decentComparator.class);
	
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}