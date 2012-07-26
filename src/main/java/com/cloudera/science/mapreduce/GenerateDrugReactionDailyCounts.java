package com.cloudera.science.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GenerateDrugReactionDailyCounts {

	public static Pattern pipeSplit = Pattern.compile("\\$");
	
	public static class CustomMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		
		Text newKey = new Text();
		Text newValue = new Text();
		
		//These are the column index from the original file
		public static final int DRUG_IDX = 0; 
		public static final int REAC_IDX = 1; 
		public static final int ISR_IDX = 2; 
		public static final int GENDER_IDX = 3;
		public static final int AGE_IDX = 4;
		public static final int TIME_IDX = 5;
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] columns = pipeSplit.split(value.toString());
			
			//for record counts
			//1. group key (gender, time, age)
			//2. isr
			//3. reacation
			try
			{
				newKey.set(columns[GENDER_IDX] + columns[TIME_IDX] +  columns[AGE_IDX] + "$" + columns[REAC_IDX] + "$" + columns[ ISR_IDX]);
				newValue.set(columns[DRUG_IDX]);
				context.write(newKey, newValue);
				
				//So that we get the group count before we start counting the records
				newKey.set(columns[GENDER_IDX] + columns[TIME_IDX] +  columns[AGE_IDX] + "$0$" + columns[ ISR_IDX]);
				newValue.set(columns[DRUG_IDX] + "$" + columns[REAC_IDX]);
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
		
		//Indexes for the content coming from the mapper
		//key indexes
		public static final int GROUP_KEY_IDX = 0; // a group is a gender, age, time
		public static final int REAC_KEY_IDX = 1;
		public static final int ISR_KEY_IDX = 2; 
		//value indexes
		public static final int DRUG_VAL_IDX = 0;
		public static final int REAC_VAL_IDX = 1;
		
		//These will keep track of the most current values
		String mostCurrentGroupKey = "";
		String mostCurrentReac = "";
		
		//This will keep the counts for all the drugs with in one group
		HashMap<String, Counter> drugPerGroupMap = new HashMap<String, Counter>();
		//This will keep the counts for all the reactions with in one group
		HashMap<String, Counter> reacPerGroupMap = new HashMap<String, Counter>();
		//This will keep all the drug pars with in a given group and given reactions
		HashMap<String, Counter> d1d2PerReacPerGroupMap = new HashMap<String, Counter>();
		
		//This will hold unique drugs for a given isr for a given reac for a given group
		HashSet<String> drugsPerIsrPreDaySet = new HashSet<String>();
		HashSet<String> reacsPerIsrPreDaySet = new HashSet<String>();
		HashSet<String> drugsPerIsrPerReacSet = new HashSet<String>();
		
		//The total records with in a group
		int totalGroupRecordCount = 0;
		
		//The total record with in a reaction
		int totalGroupReacRecordCount = 0;
		
		@Override
		public void cleanup(Context context)  throws IOException, InterruptedException
		{
			processMostCurrentRecords(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String[] keySplit = pipeSplit.split(key.toString()); 
			
			//newValue.set("--Output:" + key.toString() );
			//context.write(newKey, newValue);
			
			if (mostCurrentGroupKey.equals(keySplit[GROUP_KEY_IDX]) == false)
			{
				context.getCounter("Debug", "Group Key Change").increment(1);
				
				//write out records
				processMostCurrentRecords(context);
				
				//reset values
				mostCurrentReac = "";
				
				totalGroupRecordCount = 0;
				totalGroupReacRecordCount = 0;
				
				drugPerGroupMap.clear();
				reacPerGroupMap.clear();
				
				d1d2PerReacPerGroupMap.clear();
				
				//setup for next round
				//newValue.set("-NewGroup:" + mostCurrentGroupKey + "$" + keySplit[GROUP_KEY_IDX] + "$" + totalGroupRecordCount + "$" + drugPerGroupMap.size() + "$" + reacPerGroupMap.size());
				//context.write(newKey, newValue);
				
				mostCurrentGroupKey = keySplit[GROUP_KEY_IDX];
				
			}
			
			//check if we are counting the groups total records and type records
			if (keySplit[REAC_KEY_IDX].equals("0"))
			{
				
				context.getCounter("Debug", "Records Being Counted").increment(1);
				//add one per isr
				totalGroupRecordCount++;
				
				
				
				//We only want unique ISR
				for (Text value: values)
				{
					String[] valueSplit = pipeSplit.split(value.toString()); 
					drugsPerIsrPreDaySet.add(valueSplit[0]);
					reacsPerIsrPreDaySet.add(valueSplit[1]);
				}
					
				for (String drug: drugsPerIsrPreDaySet)
				{
					//add drugs for the group
					Counter counter = drugPerGroupMap.get(drug);
					if (counter == null)
					{
						counter = new Counter();
						drugPerGroupMap.put(drug, counter);
					}
					counter.count++;
				}
				drugsPerIsrPreDaySet.clear();
				
				for (String reac: reacsPerIsrPreDaySet)
				{
					//add reac for the group
					Counter counter = reacPerGroupMap.get(reac);
					if (counter == null)
					{
						counter = new Counter();
						reacPerGroupMap.put(reac, counter);
					}
					counter.count++;
					
				}
				reacsPerIsrPreDaySet.clear();
				
				//newValue.set("-RowCount:" + keySplit[GROUP_KEY_IDX] + "$" + keySplit[REAC_KEY_IDX] + "$" + totalGroupRecordCount + "$" + drugPerGroupMap.size() + "$" + reacPerGroupMap.size());
				//context.write(newKey, newValue);
				
			}else
			{
				//When we switch reactions we need to write stuff out.
				if (mostCurrentReac.equals(keySplit[REAC_KEY_IDX]) == false)
				{
					context.getCounter("Debug", "Reac per day ").increment(1);
					
					processMostCurrentRecords(context);
					
					//newValue.set("-ReacFlush:" + keySplit[GROUP_KEY_IDX] + "$" + mostCurrentReac + "$" + keySplit[REAC_KEY_IDX] + "$" + d1d2PerReacPerGroupMap.size() + "$" + totalGroupReacRecordCount);
					//context.write(newKey, newValue);
					
					mostCurrentReac = keySplit[REAC_KEY_IDX];
					d1d2PerReacPerGroupMap.clear();
					totalGroupReacRecordCount = 0;
					
					
				}
				
				//add reac counts
				for (Text value: values)
				{
					drugsPerIsrPerReacSet.add(value.toString());
					totalGroupReacRecordCount++;
				}
				
				context.getCounter("Debug", "isr per reac ").increment(1);
				
				String[] drugsPerIsrPerReacArray = drugsPerIsrPerReacSet.toArray(new String[0]);
				for (int i = 0 ; i < drugsPerIsrPerReacArray.length; i++)
				{
					String d1 = drugsPerIsrPerReacArray[i];
					for (int j = i+1 ; j < drugsPerIsrPerReacArray.length; j++)
					{
						String d2 = drugsPerIsrPerReacArray[j];
						
						//make sure the drug compbo is always in the same order.
						String d1d2Key = d1.compareTo(d2)<0?d1+"$"+d2:d2+"$"+d1;
						
						Counter counter = d1d2PerReacPerGroupMap.get(d1d2Key);
						if (counter == null)
						{
							counter = new Counter();
							d1d2PerReacPerGroupMap.put(d1d2Key, counter);
						}
						counter.count++;
					}
				}
				
				drugsPerIsrPerReacSet.clear();
					
				
				
				
				
			}
		}
		
		/**
		 * Simple string split to separate the group key into gender, time, age separated by '$' 
		 * 
		 * @param groupKey
		 * @return
		 */
		protected String seperatedGroupKeyWithDollarSigns(String groupKey)
		{
			return groupKey.substring(0, 1) + "$" + groupKey.substring(9) +".0$"+ groupKey.substring(1, 9)  ;
		}
		
		/**
		 * This does a dump of the most current records
		 * 
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void processMostCurrentRecords(Context context) throws IOException, InterruptedException
		{
			if (mostCurrentReac.isEmpty() == false)
			{
				context.getCounter("Debug", "able to Write ").increment(1);
						
				//TODO This should be moved up a level
				String groupKeyOutputFormat = seperatedGroupKeyWithDollarSigns(mostCurrentGroupKey);
				
				
				//drugs must be different
				for (Entry<String, Counter> entry: d1d2PerReacPerGroupMap.entrySet())
				{
					String[] keySplit = pipeSplit.split(entry.getKey()); 
					
					String drug1 = keySplit[0];
					String drug2 = keySplit[1];
					
					//group $ drug1 $ drug2 $ reac $ drug1/drug2/reac count $ group count $ drug1 count for group $ drug2 count for group $ reac count for group
					
					newValue.set(groupKeyOutputFormat + "$" + 
								 drug1 + "$" + 
								 drug2 + "$" + 
				                 mostCurrentReac + "$" + 
							     entry.getValue().count + "$" + 
							     totalGroupRecordCount + "$" + 
							     drugPerGroupMap.get(drug1) + "$" + 
							     drugPerGroupMap.get(drug2) + "$" + 
							     reacPerGroupMap.get(mostCurrentReac)) ;
					
					context.write(newKey, newValue);
				}
			}else
			{
				context.getCounter("Debug", "Unable to Write " + mostCurrentReac.isEmpty()).increment(1);
			}
		}
	}
	
	public static class CustomPartitioner extends Partitioner<Text, Text>
	{

		public static final int GROUP_KEY_IDX = 0;		
		
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			
			if (numPartitions == 1)
			{
				return 0;
			}else
			{
				//skip the sorting flags
				String[] keySplit = pipeSplit.split(key.toString()); 
							
				String partKey = keySplit[GROUP_KEY_IDX] ;
				
				int result = partKey.hashCode() % numPartitions;
				
				return Math.abs(result);
			}
		}
		
	}

	public static void main(String[] args) throws Exception
	{
		if (args.length != 3 || args[0].contains("-h"))
		{
			System.out.println("GenerateDrugReactionDailyCounts <inputPath> <outputPath> <# reducers>");
			System.out.println();
			System.out.println("GenerateDrugReactionDailyCounts strat_drugs1_reacs ./actual_expected 10");
			return;
		}

		//Get values from args
		String inputPath = args[0];
		String outputPath = args[1];
		String numberOfReducers = args[2];
		
		//Create job
		Job job = new Job();
		job.setJarByClass(GenerateDrugReactionDailyCounts.class);
		//Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		//Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);

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
