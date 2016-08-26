import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import java.lang.Math;
import java.util.List;
import java.util.ArrayList;

public class QFDMatcherReducer extends Reducer<IntWritable, WebTrafficRecord, RequestReplyMatch, NullWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<WebTrafficRecord> values, Context ctxt)
        throws IOException, InterruptedException {

        // The input is a set of WebTrafficRecords for each key,
        // the output should be the WebTrafficRecords for
        // all cases where the request and reply are matched
        // as having the same
        // Source IP/Source Port/Destination IP/Destination Port
        // and have occured within a 10 second window on the timestamp.

        // One thing to really remember, the Iterable element passed
        // from hadoop are designed as READ ONCE data, you will
        // probably want to copy that to some other data structure if
        // you want to iterate mutliple times over the data.

        //System.err.println("Need to implement!");

        // ctxt.write should be RequestReplyMatch and a NullWriteable

        //requests contain cookies
        //replies contain usernames

        List<WebTrafficRecord> permanent = new ArrayList<>();
        for (WebTrafficRecord wtr : values) {
            permanent.add(new WebTrafficRecord(wtr));
        }

        for (WebTrafficRecord outside : permanent) {
          for (WebTrafficRecord inside : permanent) {
            WebTrafficRecord outsideWTR = new WebTrafficRecord(outside);
            WebTrafficRecord insideWTR = new WebTrafficRecord(inside);
            if ((!(outsideWTR.equals(insideWTR))) && (outsideWTR.tupleMatches(insideWTR)) && (Math.abs(outsideWTR.getTimestamp() - insideWTR.getTimestamp()) <= 10)){
              if ((outsideWTR.getCookie() == null) && (outsideWTR.getUserName() != null) && (insideWTR.getUserName() == null) && (insideWTR.getCookie() != null)) {
                RequestReplyMatch paired = new RequestReplyMatch(insideWTR, outsideWTR);
                NullWritable nw = NullWritable.get();
                ctxt.write(paired, nw);
              } else if ((outsideWTR.getCookie() != null) && (outsideWTR.getUserName() == null) && (insideWTR.getUserName() != null) && (insideWTR.getCookie() == null)) {
                RequestReplyMatch paired = new RequestReplyMatch(outsideWTR, insideWTR);
                NullWritable nw = NullWritable.get();
                ctxt.write(paired, nw);
              }
            }
          }
        }
    }
}

//Match up requests with replies, and emit them as RequestReplyMatch instances.
