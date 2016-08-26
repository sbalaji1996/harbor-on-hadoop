import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class QFDMatcherMapper extends Mapper<LongWritable, Text, IntWritable, WebTrafficRecord> {

    @Override
    public void map(LongWritable lineNo, Text line, Context ctxt)
				throws IOException, InterruptedException {
        // Inputs come on lines of text that can be parsed
        // as WebTrafficRecord, your key should be such that all
        // records with the same source IP/source port/dest IP/dest port
        // are the same so they always go to the same reducer...
        //System.err.println("Need to implement!");

				String lineParsed = line.toString();
				WebTrafficRecord parsedFromString = WebTrafficRecord.parseFromLine(lineParsed);
				int hashedValue = parsedFromString.matchHashCode(); //use matchHashCode because the matching reply/request pairs are within 10 seconds of each other, not at the exact same time
				ctxt.write(new IntWritable(hashedValue), parsedFromString);

				//put into hash table based on attributes of the WebTrafficRecord? then check using the hash table to see how to return matched values

    }
}

//Create a new WebTrafficRecord from the input data and emit it along with a key that ensures that all matching request/reply pairs go to the same reducer.
