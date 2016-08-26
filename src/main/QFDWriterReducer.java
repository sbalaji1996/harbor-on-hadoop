import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.ObjectOutputStream;

import java.util.Set;
import java.util.HashSet;

public class QFDWriterReducer extends Reducer<WTRKey, RequestReplyMatch, NullWritable, NullWritable> {

    @Override
    public void reduce(WTRKey key, Iterable<RequestReplyMatch> values, Context ctxt)
        throws IOException, InterruptedException {

        // The input will be a WTR key and a set of matches.

        // You will want to open the file named
        // "qfds/key.getName()/key.getName()_key.getHashBytes()"
        // using the FileSystem interface for Hadoop.

        // EG, if the key's name is srcIP and the hash is 2BBB,
        // the filename should be qfds/srcIP/srcIP_2BBB

        // Some useful functionality:

        // FileSystem.get(ctxt.getConfiguration())
        // gets the interface to the filesysstem
        // new Path(filename) gives a path specification
        // hdfs.create(path, true) will create an
        // output stream pointing to that file

        //Create a QueryFocuseDataSet from the request/reply matches and serialize the QFD object to a file on HDFS based on name and hash bytes.

        Set<RequestReplyMatch> permanent = new HashSet<RequestReplyMatch>(); //Collections.emptySet(); // = new Set<RequestReplyMatch>();
        for (RequestReplyMatch rrm : values) {
            permanent.add(new RequestReplyMatch(rrm));
        }

        String keyName = key.getName();
        String keyBytes = key.getHashBytes();
        QueryFocusedDataSet qfdsHold = new QueryFocusedDataSet(keyName, keyBytes, permanent);

        FileSystem hdfs = FileSystem.get(ctxt.getConfiguration());
        String pathName = "qfds/" + key.getName() + "/" + key.getName() + "_" + key.getHashBytes();
        Path pathToFile = new Path(pathName);
        FSDataOutputStream pointOutputStream = hdfs.create(pathToFile, true);
        ObjectOutputStream oStream = new ObjectOutputStream(pointOutputStream);
        oStream.writeObject(qfdsHold);
        oStream.close();
        pointOutputStream.close();

        NullWritable nw = NullWritable.get();
        ctxt.write(nw, nw);
    }
}
