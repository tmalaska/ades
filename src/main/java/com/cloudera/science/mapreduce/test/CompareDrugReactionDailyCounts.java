package com.cloudera.science.mapreduce.test;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.cloudera.science.mapreduce.GenerateDrugReactionDailyCounts;

public class CompareDrugReactionDailyCounts 
{

	public static Pattern pipeSplit = Pattern.compile("\\$");
	
	public static class CustomMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		
		//group $ drug1 $ drug2 $ reac $ drug1/drug2/reac count $ group count $ drug1 count for group $ drug2 count for group $ reac count for group
		public static final int GENDER_IDX = 0;
		public static final int AGE_IDX = 1;
		public static final int TIME_IDX = 2;
		public static final int DRUG1_IDX = 3;
		public static final int DRUG2_IDX = 4;
		public static final int REAC_IDX = 5;
		public static final int D1D2RC_COUNT_IDX = 6;
		public static final int GROUP_COUNT = 7;
		public static final int DRUG1_COUNT = 8;
		public static final int DRUG2_COUNT = 9;
		public static final int REC_COUNT = 10;
		
		Text newKey = new Text();
		Text newValue = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] columns = pipeSplit.split(value.toString());
			
			try
			{	
				if (columns[GENDER_IDX].startsWith("0"))
				{
					columns[GENDER_IDX] = columns[GENDER_IDX].substring(columns[GENDER_IDX].indexOf("\t") + 1);
					newValue.set("N");
				}else
				{
					newValue.set("O");
				}
				
				if (columns[DRUG1_IDX].compareTo(columns[DRUG2_IDX])< 0)
				{
					newKey.set(columns[GENDER_IDX] + 
							   columns[AGE_IDX] +  
							   columns[TIME_IDX] + "$" + 
							   columns[DRUG1_IDX] + "$" + 
							   columns[DRUG2_IDX] + "$" + 
							   columns[REAC_IDX] + "$" + 
							   columns[D1D2RC_COUNT_IDX] + "$" + 
							   columns[GROUP_COUNT] + "$" + 
							   columns[DRUG1_COUNT] + "$" + 
							   columns[DRUG2_COUNT] + "$" + 
							   columns[REC_COUNT] 
							);
						
				}else
				{
					newKey.set(columns[GENDER_IDX] + 
							   columns[AGE_IDX] +  
							   columns[TIME_IDX] + "$" + 
							   columns[DRUG2_IDX] + "$" + 
							   columns[DRUG1_IDX] + "$" + 
							   columns[REAC_IDX] + "$" + 
							   columns[D1D2RC_COUNT_IDX] + "$" + 
							   columns[GROUP_COUNT] + "$" + 
							   columns[DRUG2_COUNT] + "$" + 
							   columns[DRUG1_COUNT] + "$" + 
							   columns[REC_COUNT] 
							);
				}
				
				context.write(newKey, newValue);
				
				
			}catch (Exception e)
			{
				throw new RuntimeException("Problem reading: " + value.toString(), e);
			}
		}
	}
	
	public static class CustomReducer extends Reducer<Text, Text, LongWritable, Text>
	{
		LongWritable newKey = new LongWritable();
		Text newValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int nCount = 0;
			int oCount = 0;
			
			for (Text value: values)
			{
				if (value.toString().equals("N"))
				{
					nCount++;
				}else if (value.toString().equals("O"))
				{
					oCount++;
				} 
			}
			
			if (oCount != nCount)
			{
				newValue.set(key.toString() + " N:" + nCount + " O:" + oCount);
				
				context.write(newKey, newValue);
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args)  throws Exception{
		if (args.length != 4 || args[0].contains("-h"))
		{
			System.out.println("CompareDrugReactionDailyCounts <inputPath> <inputPath2> <outputPath> <# reducers>");
			System.out.println();
			System.out.println("CompareDrugReactionDailyCounts ./new ./old ./results 10");
			return;
		}

		//Get values from args
		String inputPath = args[0];
		String inputPath2 = args[1];
		String outputPath = args[2];
		String numberOfReducers = args[3];
		
		//Create job
		Job job = new Job();
		job.setJarByClass(GenerateDrugReactionDailyCounts.class);
		//Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileInputFormat.addInputPath(job, new Path(inputPath2));
		
		//Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);


		// Define the key and value format
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(numberOfReducers));

		// Exit
		job.waitForCompletion(true);
	}

}
