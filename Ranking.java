package org.myorg;

/*
 * Rashi Jain
 * rjain12@uncc.edu
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Ranking extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Ranking.class);

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new Ranking(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

		// setting the temporary folder to store the files
		String tempDirectory = args[1].trim()+"/temp";
		int status;
		String sorterInputPath = null;

		// first job to calculate the number of pages in the corpus
		Job jobCount  = Job .getInstance(getConf(), " ranking ");
		jobCount.setJarByClass( this .getClass());

		Configuration configuration= new Configuration();
		FileSystem fs = FileSystem.get(configuration);

		/*
		 * Deeleting the temporary and the output folder
		 * if these folder already exists
		 */
		if(fs.exists(new Path(args[1]))){
			fs.delete(new Path(args[1]), true);
		}
		if(fs.exists(new Path(tempDirectory))){
			fs.delete(new Path(tempDirectory), true);
		}
		if(fs.exists(new Path(args[2]))){
			fs.delete(new Path(args[2]), true);
		}
		FileInputFormat.addInputPaths(jobCount,  args[0]);
		FileOutputFormat.setOutputPath(jobCount,  new Path(args[1]));
		jobCount.setMapperClass( MapCount .class);
		jobCount.setReducerClass( ReduceCount .class);
		jobCount.setOutputKeyClass( Text .class);
		jobCount.setOutputValueClass( IntWritable .class);

		status = jobCount.waitForCompletion( true)?0:1;

		int numberOfPages=0;

		if(status == 0){
			try{
				Path p = new Path(args[1]);
				// fetching the output file of the first job
				Path pt=new Path(p,"part-r-00000");
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;

				//reading the lines from the output file generated
				line=br.readLine().trim();
				while (line != null){ 	
					if(line.trim().length()>0){
						numberOfPages++;		
					}
					line= br.readLine().trim();
				}
			}catch(Exception e){
			}
			Configuration config2 = new Configuration();
			config2.setInt("numberOfPages",numberOfPages);
			System.out.println("Total Pages in the corpus is "+numberOfPages);

			// running the second job for creation of graph
			Job jobGraph  = Job .getInstance(config2, "graph");
			jobGraph.setJarByClass( this .getClass());		
			jobGraph.setMapperClass( MapGraph.class);
			jobGraph.setReducerClass( ReduceGraph .class);

			FileInputFormat.addInputPaths(jobGraph,  args[1]);
			//System.out.println("#####################"+tempDirectory);
			FileOutputFormat.setOutputPath(jobGraph,  new Path(tempDirectory));

			jobGraph.setOutputKeyClass( Text .class);
			jobGraph.setOutputValueClass( Text .class);
			status = jobGraph.waitForCompletion( true)?0:1;
			Path TempOutput;
			String input;

			/*
			 * running the 3rd job 10 times
			 * for calculating the page ranks of the pages of the corpus
			 */
			if(status ==0){
				for(int i=1; i<=10;i++){
					System.out.println("--------------Inside loop------------------");
					Job jobRank  = Job .getInstance(getConf(), " Rank ");
					Configuration config = jobRank.getConfiguration();
					jobRank.setJarByClass( this .getClass());
					TempOutput = new Path(tempDirectory+String.valueOf(i));
					System.out.println("--------------------------output file"+TempOutput);

					/* 
					 * if it is the initial iteration
					 * then take the input path as the output path created from 2nd job
					 */
					if(i==1)
						input = tempDirectory;	
					else
						input = tempDirectory+String.valueOf(i-1);

					// to delete the file if it already exists
					if(fs.exists(TempOutput)){
						System.out.println("-----------output directory exists-----------------------------");
						fs.delete(TempOutput,true);
					}
					FileInputFormat.addInputPaths(jobRank,  input);
					FileOutputFormat.setOutputPath(jobRank,  TempOutput);

					jobRank.setMapperClass( MapRank .class);
					jobRank.setReducerClass( ReduceRank .class);
					jobRank.setOutputKeyClass( Text .class);
					jobRank.setOutputValueClass( Text .class);
					sorterInputPath = TempOutput.toString();
					status= jobRank.waitForCompletion( true)?0:1;
				}
				if(status == 0){
					System.out.println("----sorter input path is "+sorterInputPath);
					System.out.println("----running job3----");

					/*
					 * 3rd job for sorting the pages
					 * according to the ranks
					 */
					Job job3  = Job .getInstance(getConf(), " pagerankSorting ");
					Configuration config = job3.getConfiguration();
					job3.setJarByClass( this .getClass());
					FileSystem fsystem = FileSystem.get(config);

					FileInputFormat.addInputPaths(job3,  sorterInputPath);
					FileOutputFormat.setOutputPath(job3,  new Path(args[2]));
					job3.setNumReduceTasks(1);
					job3.setSortComparatorClass(DescendingComparator.class);

					job3.setMapperClass( PageMapSorter .class);
					job3.setReducerClass( PageReduceSorter .class);
					job3.setOutputKeyClass( DoubleWritable .class);
					job3.setOutputValueClass( Text .class);
					status= job3.waitForCompletion( true)?0:1;

				}
			}
		}	
		return status;
	}

	// 1st Job to calculate the number of pages in the corpus
	public static class MapCount extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);
		private Text word  = new Text();

		// this mapper will just set the output as it in the same form
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			System.out.println(lineText);
			String line  = lineText.toString();
			if(!line.isEmpty())
				context.write(lineText,one);
		}
	}

	public static class ReduceCount extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int numberOfFiles  = 0;
			for ( IntWritable count  : counts) {
				numberOfFiles  += count.get();
			}
			context.write(word,  new IntWritable(numberOfFiles));
		}
	}

	// 2nd job to calculate the Graphs (parsing of xml is done)
	public static class MapGraph extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		/*
		 * output is of the form: url	pageRank######outlink1@@@@outlink2@@@@
		 * e.g. abc 0.5#####qq@@@@34@@@@
		 */
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			String line  = lineText.toString();
			String url="";
			System.out.println("inside Mapper ------"+context.getConfiguration().get("numberOfPages"));
			
			// getting the number of files set from the first job
			double numberOfPages = Double.parseDouble(context.getConfiguration().get("numberOfPages"));
			
			/* 
			 * setting the initial rank of all the pages to 1/N
			 * where N = number of pages in the corpus
			 */
			double initialPageRank = 1/(Double)numberOfPages;
			System.out.println("No of pages are---"+numberOfPages);

			// fetching the values of the url based on the <title> 
			Pattern pattern = Pattern.compile("<title>(.*)</title>");
			Matcher matcher = pattern.matcher(line);
			if (matcher.find()){
				url = matcher.group(1);
			}

			// checking the <text> from xml
			Pattern patternText = Pattern.compile("<text.*?>(.*)</text>");
			Matcher matcherText = patternText.matcher(line);
			String RankAndOutLinks="";
			RankAndOutLinks = Double.toString(initialPageRank) + "######";
			if (matcherText.find()){

				// checking the outlinks [[]]
				Pattern patternOutlink = Pattern.compile("\\[\\[(.*?)\\]\\]");
				Matcher matcherOutlink = patternOutlink.matcher(matcherText.group(1));
				while(matcherOutlink.find()){
					String link = matcherOutlink.group(1);
					RankAndOutLinks += link+"@@@@";
				}
			}

			System.out.println("URL is "+url);
			System.out.println("matcher is "+RankAndOutLinks);
			if(!line.isEmpty())
				context.write(new Text(url), new Text(RankAndOutLinks));
		}
	}

	/*
	 * Input is of the form: url	[pageRank######outlink1@@@@outlink2@@@@]
	 * e.g. abc [0.5#####qq@@@@34@@@@]
	 */
	public static class ReduceGraph extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {
			//int numberOfFiles  = 0;
			
			// this reducer will just write the input to the output
			System.out.println("---------------inside reducer");
			System.out.println(word);
			for ( Text value  : counts) {
				System.out.println("------ value is "+value);
				context.write(word,  value);
			}
		}
	}

	// 3rd job to calculate the rank of the pages of the corpus
	public static class MapRank extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		/*
		 * output from the mapper:
		 * url	pagerank
		 * url pagerank######outlinks@@@@outlinks@@@@
		 */
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			System.out.println(lineText);
			String line  = lineText.toString();
			String url = line.split("\t")[0];
			String values = line.split("\t")[1];
			String pageRank="";
			String Totallinks="";
			String[] imValues = values.split("\\######");
			pageRank = values.split("\\######")[0];

			// if the length is 2, it will check for pagerank and the outlinks
			if(imValues.length==2){
				Totallinks = values.split("\\######")[1];		
				System.out.println(pageRank);
				System.out.println(Totallinks);
				String[] outlinks = Totallinks.split("@@@@");
				if(outlinks.length>0){
					for(String link: outlinks){
						// calculation of pagerank by taking the initialranks/ length_of_outlinks
						Double v = Double.parseDouble(pageRank)/outlinks.length;
						context.write(  new Text(link), new Text(String.valueOf(v)));
					}
				}
			}
			context.write(new Text(url), new Text(values));
		}
	}

	/*
	 * input of the reducer:
	 * url	[pagerank pagerank pagerank######outlinks@@@@outlinks@@@@]
	 */
	public static class ReduceRank extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {
			double pageRankTemp=0.0;
			double pageRank=0.0;
			double dampingFactor = 0.85;
			boolean outlinkExist = false;
			String outlinks = "";

			for ( Text value  : counts) {	
				if(value.toString().contains("######")){
					outlinkExist =true;
					String v[] = value.toString().split("######");
					if(v.length == 2)
						outlinks = value.toString().split("######")[1];
				}
				else
					pageRankTemp += Double.parseDouble(value.toString());
			}

			// ignore if page is not present by checking the outlinks of that particular node
			if(!outlinkExist)
				return;
			pageRank = (1-dampingFactor)+ dampingFactor*pageRankTemp;
			String finalValue = pageRank+"######"+ outlinks;
			System.out.println(word+ "\t" +finalValue );
			context.write(word,  new Text(finalValue));
		}
	}

	// 4th job for sorting the page ranks of the corpus
	public static class PageMapSorter extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {

		/*
		 * output of the mapper is:
		 * pagerank	url
		 * e.g. 0.898 abc
		 */
		public void map( LongWritable text,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();
			String url = line.split("\t")[0];
			String values = line.split("\t")[1];
			String pageRank = values.split("######")[0];
			System.out.println("------"+pageRank+"*******"+url);
			context.write(new DoubleWritable(Double.parseDouble(pageRank)), new Text(url));

		}
	}

	/*
	 * output is of the form url pagerank
	 * e.g. abc  0.989
	 */
	public static class PageReduceSorter extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( DoubleWritable word,  Iterable<Text > url,  Context context)
				throws IOException,  InterruptedException {
			for ( Text value  : url) {
				context.write(value,  word);
			}
		}
	}

	// sorter which will sort the pagerank values in descending order
	public static class DescendingComparator extends WritableComparator {
		protected DescendingComparator() {
			super(DoubleWritable.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable key1 = (DoubleWritable) w1;
			DoubleWritable key2 = (DoubleWritable) w2;          
			return -1 * key1.compareTo(key2);
		}
	}

}