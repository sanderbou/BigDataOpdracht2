package org.myorg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class InputConverterAndPageLink extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(InputConverterAndPageLink.class);

	private static BigDecimal count_to_nodes = new BigDecimal(0);
	private static BigDecimal totaalAantalNodes = new BigDecimal(62586);
	private static BigDecimal HETGetal = new BigDecimal(1000);
	private static BigDecimal total = new BigDecimal(0);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new InputConverterAndPageLink(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "InputConverter");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map_Converter.class);
		// job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce_Converter.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(getConf(), "PageLink");
		job2.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(Map_PageLink.class);
		job2.setCombinerClass(Reduce_PageLink.class);
		job2.setReducerClass(Reduce_PageLink.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
        
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map_Converter extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private String input;

		protected void setup(Mapper.Context context) throws IOException,
				InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				this.input = ((FileSplit) context.getInputSplit()).getPath()
						.toString();
			} else {
				this.input = context.getInputSplit().toString();
			}
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			Text toNode = new Text();
			Text fromNode = new Text(line.split("\t")[0]);

			for (String word : line.split("\t")) {
				if (word.isEmpty()) {
					continue;
				}
				toNode = new Text(word);
				if (!toNode.equals(fromNode)) {
					context.write(fromNode, toNode);
				}
			}
		}
	}

	public static class Reduce_Converter extends
			Reducer<Text, Text, Text, List<Text>> {
		public void reduce(Text fromNode, Iterable<Text> toNodes,
				Context context) throws IOException, InterruptedException {
			HashMap<Text, List<Text>> fromNodeToNodes = new HashMap<Text, List<Text>>();
			List<Text> theToNodes = new ArrayList<Text>();

			for (Text toNode : toNodes) {
				Text insertToNode = new Text(toNode);
				if (fromNodeToNodes.get(fromNode) == null
						&& fromNodeToNodes.get(toNode) == null) {
					fromNodeToNodes.put(fromNode, theToNodes);
					fromNodeToNodes.get(fromNode).add(insertToNode);
				} else {
					fromNodeToNodes.get(fromNode).add(insertToNode);
				}
			}

			for (Entry<Text, List<Text>> hashMapEntry : fromNodeToNodes
					.entrySet()) {
				Text fromNodeEntry = hashMapEntry.getKey();
				List<Text> toNodesEntry = hashMapEntry.getValue();
				context.write(fromNodeEntry, toNodesEntry);
				//totaalAantalNodes = totaalAantalNodes.add(new BigDecimal(1));
			}
		}
	}

	public static class Map_PageLink extends Mapper<LongWritable, Text, Text, Text> {
		private String input;
		protected void setup(Mapper.Context context) throws IOException,
				InterruptedException {
			if (context.getInputSplit() instanceof FileSplit) {
				this.input = ((FileSplit) context.getInputSplit()).getPath()
						.toString();
			} else {
				this.input = context.getInputSplit().toString();
			}
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			Text fromNode = new Text(line.split("\t")[0]);
			String toNodes = line.split("\t")[1];
			Text pageLinkValueNode = new Text();
			
			toNodes = toNodes.replace("[","").replace("]", "");
			count_to_nodes = new BigDecimal(0);
			
			for(String node : toNodes.split(", ")){
				count_to_nodes = count_to_nodes.add(new BigDecimal(1));
			}
			
			BigDecimal firstDivide = HETGetal.divide(totaalAantalNodes,20,BigDecimal.ROUND_DOWN);
			BigDecimal value = firstDivide.divide(count_to_nodes,20,BigDecimal.ROUND_DOWN);
			value.add(firstDivide);l
			pageLinkValueNode = new Text(value + "");
			
			for(String node : toNodes.split(", ")){
				context.write(new Text(node), pageLinkValueNode);
			}
		}
	}

	public static class Reduce_PageLink extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text toNode, Iterable<Text> pageLinkValueNode, Context context) throws IOException, InterruptedException {
			BigDecimal sum = new BigDecimal(0);
			for(Text pagelinkValue : pageLinkValueNode){
				BigDecimal value = new BigDecimal(pagelinkValue.toString().replaceAll(",",""));
				sum = sum.add(value);
			}
			total = total.add(sum);
			System.out.println(total);
			context.write(toNode, new Text(sum+""));
		}
	}
}