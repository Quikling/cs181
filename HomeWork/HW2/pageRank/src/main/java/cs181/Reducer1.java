package cs181;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
 
public class Reducer1 extends Reducer<Text, Text, Text, Text> {

    /* TODO - Implement the reduce function. 
     * 
     * 
     * Input :    Adjacency Matrix Format       ->  ( j   ,   M  \t  i  \t value )
     *            Vector Format                 ->  ( j   ,   V  \t   value )
     * 
     * Output :   Key-Value Pairs               
     *            Key ->    i
     *            Value ->  M_ij * V_j  
     *                      
     */

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
        
        double vVal = 0;
        ArrayList<String> mList = new ArrayList<String> ();
                    
        // Loop through values, to add m_ij term to mList and save v_j to variable v_j
        // Then Iterate through the terms in mList, to multiply each term by variable v_j.
        // Each output is a key-value pair  ( i  ,   m_ij * v_j)
        for (Text val : values) {
            
            String input = val.toString();
            String[] valArr = input.split("\t");
            
            if (valArr[0].equals("M")) {
                mList.add(valArr[1] + "\t" + valArr[2]);
            }
            else {
                vVal = Double.parseDouble(valArr[1]);
            }
        }
        
        for (String text : mList) {
        	System.out.println(text);
        }
        
        for (String m_ij : mList) {
            
            String[] mArr = m_ij.split("\t");
            
            Text m_i = new Text(mArr[0]);
            double mv = Double.parseDouble(mArr[1]) * vVal;
            Text mv_text = new Text(Double.toString(mv));
            
            context.write(m_i, mv_text);
        }
        
    }

}
