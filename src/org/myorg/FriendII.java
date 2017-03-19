package org.myorg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.io.IOException;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class FriendII extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(FriendII.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new FriendII(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "FriendII");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private String input;

    protected void setup(Mapper.Context context)
        throws IOException,
        InterruptedException {
      if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }
    }

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
      String line = lineText.toString();
      Text fromNode =  new Text(line.split("\t")[0]);
      Text toNode;
      
      for (String toNodeString : line.split("\t")) {
        if (toNodeString.isEmpty()) {
            continue;
        }
        toNode = new Text(toNodeString);
        if(!toNode.equals(fromNode)){
        	context.write(fromNode,toNode);
         }
       }             
    }
  }
  
  public static class Combiner extends Reducer<Text,Text, Text,List<Text>> {
      public void reduce(Text fromNode, Iterable<Text> toNodes, Context context)throws IOException, InterruptedException{
      	HashMap<Text, List<Text>> fromNodeToNodes = new HashMap<Text, List<Text>>();
      	List<Text> theToNodes = new ArrayList<Text>();
      	
      	for(Text toNode : toNodes){
      		Text insertToNode = new Text(toNode);
      		if(fromNodeToNodes.get(fromNode)==null && fromNodeToNodes.get(toNode)==null){
      			fromNodeToNodes.put(fromNode, theToNodes);
      			fromNodeToNodes.get(fromNode).add(insertToNode); 
      		}else{
      			fromNodeToNodes.get(fromNode).add(insertToNode);	
      		}
      	}
      	
      	for (Entry<Text, List<Text>> hashMapEntry : fromNodeToNodes.entrySet()) {
      	    Text fromNodeEntry = hashMapEntry.getKey();
      	    List<Text> toNodesEntry = hashMapEntry.getValue();
      	    context.write(fromNodeEntry, toNodesEntry);
      	}
      }
  }
  
  public static class Reduce extends Reducer<Text, List<Text>, Text, List<Text>> {
    public void reduce(Text fromNode, List<Text> toNodes, Context context) throws IOException, InterruptedException {
    	context.write(fromNode, toNodes);    	
    	/*HashMap<Text, List<Text>> usersAndFriends = new HashMap<Text, List<Text>>();
    	HashMap<List<Text>, List<Text>> theResults = new HashMap<List<Text>, List<Text>>();
    	List<Text> theFriends = new ArrayList<Text>();
    	List<Text> resultMatchNames = new ArrayList<Text>();
    	
    	for(Text friend : friends){
    		if(usersAndFriends.get(user)==null && usersAndFriends.get(friend)==null){
    			theFriends.add(friend);
        		usersAndFriends.put(user, theFriends);
        		theFriends.remove(friend);
    		}else{
    			Text newFriend = new Text(friend);
    			usersAndFriends.get(user).add(newFriend);	
    		}
    	}
    	for (Entry<Text, List<Text>> entry : usersAndFriends.entrySet()) {
    	    Text me = entry.getKey();
    	    List<Text> myFriends = entry.getValue();
    	    //context.write(me, myFriends);
    	    
    	    for(Text myFriend : myFriends){
    	    	List<Text>myFriendsToRetain = new ArrayList<Text>(myFriends);
    	    	List<Text>otherPersonFriends = usersAndFriends.get(myFriend);
    	    	//if(otherPersonFriends != null){
	    	    	myFriendsToRetain.retainAll(otherPersonFriends);
	    	    	if(myFriendsToRetain != null){
	    	    		Text meForListSave = new Text(me);
	    				Text myFriendForListSave = new Text(myFriend);
	    				resultMatchNames.add(meForListSave);
	    				resultMatchNames.add(myFriendForListSave);
	    				theResults.put(resultMatchNames,myFriendsToRetain);
	    				resultMatchNames.remove(meForListSave);
	    				resultMatchNames.remove(myFriendForListSave);
	    	    	//}
    	    	}
    		}
    	}
    	for (Entry<List<Text>, List<Text>> entry : theResults.entrySet()) {
    	    List<Text> resultPairs = entry.getKey();
    	    List<Text> resultPairFriends = entry.getValue();
    	    context.write(resultPairs, resultPairFriends);
    	}*/
    }
  }
}