package cs181;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
   
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {      
        
    /* TODO - Implement the map function, where each call to the function receives just 1 line from the input files. 
     * Recall, we had two input files feed-in to our map reduce job, both the adjacency matrix and the vector file. 
     * This, our code must contain some logic to differentiate between the two inputs, and output the appropriate key-value pair.
     * 
     * Input :    Adjacency Matrix Format       ->  M  \t  i    \t  j       \t value 
     *            Vector Format                 ->  V  \t  j    \t  value 
     * 
     * Output :   Key-Value Pairs               
     *            Key ->    j
     *            Value ->  M   \t  i   \t  value    or   
     *                      V   \t  value 
     */

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
        

        String input  = value.toString();
        String[] indicesAndValue = input.split("\t"); // tab delimited

        String outKey;
        String outValue;
        
        if (indicesAndValue[0].equals("M")) {
            outKey = indicesAndValue[2];
            outValue = indicesAndValue[0] + "\t" + indicesAndValue[1] + "\t" + indicesAndValue[3];
        }
        else {
            outKey = indicesAndValue[1];
            outValue = indicesAndValue[0] + "\t" + indicesAndValue[2];
        }

         context.write(new Text(outKey), new  Text(outValue));
    }
}
