package cs181;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Word Count Mapper 
 * Receives lines of text, splits each line into words, and generates key, value pairs. Where 
 * the key is the word, and the value is just 1. The counts for a given key will be aggregated in the reducer. 
 *
 * @param  Raw text
 * @return < Key , 1 >
 * 
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private String pattern= "^[a-z][a-z0-9\\'\\’]*$";
	    /* 
	     * List of stopwords to compare against.
	     * Ideally this would be a hash set, but I'm not sure the overhead
	     * creating a hash set for each mapper instance would be worth it 
	     * and I'm not sure how to create a global one
	     */
	    private String[] stopWords = {"a","about","above","after","again","against","all","am","an","and","any","are","aren’t","as","at","be","because","been","before","being","below","between","both","but","by","can’t","cannot","could","couldn’t","did","didn’t","do","does","doesn’t","doing","don’t","down","during","each","few","for","from","further","had","hadn’t","has","hasn’t","have","haven’t","having","he","he’d","he’ll","he’s","her","here","here’s","hers","herself","him","himself","his","how","how’s","i","i’d","i’ll","i’m","i’ve","if","in","into","is","isn’t","it","it’s","its","itself","let’s","me","more","most","mustn’t","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours   ourselves","out","over","own","same","shan’t","she","she’d","she’ll","she’s","should","shouldn’t","so","some","such","than","that","that’s","the","their","theirs","them","themselves","then","there","there’s","these","they","they’d","they’ll","they’re","they’ve","this","those","through","to","too","under","until","up","very","was","wasn’t","we","we’d","we’ll","we’re","we’ve","were","weren’t","what","what’s","when","when’s","where","where’s","which","while","who","who’s","whom","why","why’s","with","won’t","would","wouldn’t","you","you’d","you’ll","you’re","you’ve","your","yours","yourself","yourselves"};
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        
	    	String line = value.toString();  /* get line of text from variable 'value' and convert to string */
	    	
	    	/* Lets use a string tokenizer to split line by words using a pattern matcher */
	        StringTokenizer tokenizer = new StringTokenizer(line); 
	                
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            String stringWord = word.toString().toLowerCase();
	            
	            /* 
	             * This is supposed to take out all punctuation that
	             * isn't a single quote so that we still get words like
	             * "can't" or "doesn't" and things with dashes because
	             * I wanted to take out the words like "Sawyer--she"
	             * For some reason it currently takes out single quotes 
	             * and sometimes everything after it.
	             * 
	             * It works for a sample file that I created though. Not sure why.
	             * The sample file is in the zip and on my github
	             */
	            stringWord = stringWord.replaceAll("[^\\w\\s\\-\\'\\’]+", "");
	            
	            /* for each word if it's not a stopword, output the word as the key, and value as 1 */
	            if (stringWord.matches(pattern) && !ArrayUtils.contains(stopWords, stringWord)){
	            	context.write(new Text(stringWord), one);
	            }
	            
	        }
	    }
	}