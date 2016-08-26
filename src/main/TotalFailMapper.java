import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ObjectInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

public class TotalFailMapper extends Mapper<LongWritable, Text, WTRKey, RequestReplyMatch> {

    private MessageDigest messageDigest;
    @Override
    public void setup(Context ctx) throws IOException, InterruptedException {
        // You probably need to do the same setup here you did
        // with the QFD writer
        //System.err.println("Need to write setup code!");
        super.setup(ctx);
        try {
          messageDigest = MessageDigest.getInstance("SHA-1");
        }  catch (NoSuchAlgorithmException e) {
            // SHA-1 is required on all Java platforms, so this
            // should never occur
            throw new RuntimeException("SHA-1 algorithm not available");
        }
        // Now we are adding the salt...
        messageDigest.update(HashUtils.SALT.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void map(LongWritable lineNo, Text line, Context ctxt)
            throws IOException, InterruptedException {

        // The value in the input for the key/value pair is a Tor IP.
        // You need to then query that IP's source QFD to get
        // all cookies from that IP,
        // query the cookie QFDs to get all associated requests
        // which are by those cookies, and store them in a torusers QFD

        //System.err.println("You need to put some code here!");

        String srcIPString = line.toString();

        MessageDigest mdSRC = HashUtils.cloneMessageDigest(messageDigest);
				mdSRC.update(srcIPString.getBytes(StandardCharsets.UTF_8));
				byte[] hashSRC = mdSRC.digest();
				byte[] hashBytesSRC = Arrays.copyOf(hashSRC, HashUtils.NUM_HASH_BYTES);
				String hashStringSRC = DatatypeConverter.printHexBinary(hashBytesSRC);

        String pathName = "qfds/srcIP/srcIP_" + hashStringSRC;
        FileSystem hdfs = FileSystem.get(ctxt.getConfiguration());
        Path pathToFile = new Path(pathName);
        FSDataInputStream pointInputStream = hdfs.open(pathToFile);
        ObjectInputStream iStream = new ObjectInputStream(pointInputStream);

        QueryFocusedDataSet srcReadIn = null;

        try {
          srcReadIn = (QueryFocusedDataSet) iStream.readObject();
        } catch (ClassNotFoundException e) {

        }

        Set<RequestReplyMatch> rrms = srcReadIn.getMatches();
        Set<String> cookies = new HashSet<String>();
        for (RequestReplyMatch rrm : rrms) {
          cookies.add(rrm.getCookie());
        }

        for (String cookieName : cookies) {
          MessageDigest mdCookie = HashUtils.cloneMessageDigest(messageDigest);
  				mdCookie.update(cookieName.getBytes(StandardCharsets.UTF_8));
  				byte[] hashCookie = mdCookie.digest();
  				byte[] hashBytesCookie = Arrays.copyOf(hashCookie, HashUtils.NUM_HASH_BYTES);
  				String hashStringCookie = DatatypeConverter.printHexBinary(hashBytesCookie);

          String cookiePathName = "qfds/cookie/cookie_" + hashStringCookie;
          FileSystem hdfsCookie = FileSystem.get(ctxt.getConfiguration());
          Path pathToCookie = new Path(cookiePathName);
          FSDataInputStream cookieInputStream = hdfsCookie.open(pathToCookie);
          ObjectInputStream iStreamCookie = new ObjectInputStream(cookieInputStream);

          QueryFocusedDataSet cookieReadIn = null;

          try {
            cookieReadIn = (QueryFocusedDataSet) iStreamCookie.readObject();
          } catch (ClassNotFoundException e) {

          }

          Set<RequestReplyMatch> cookierrms = cookieReadIn.getMatches();

          for (RequestReplyMatch usernames : cookierrms) {
            MessageDigest mdUserName = HashUtils.cloneMessageDigest(messageDigest);
    				mdUserName.update(usernames.getUserName().getBytes(StandardCharsets.UTF_8));
    				byte[] hashUserName = mdUserName.digest();
    				byte[] hashBytesUserName = Arrays.copyOf(hashUserName, HashUtils.NUM_HASH_BYTES);
    				String hashStringUserName = DatatypeConverter.printHexBinary(hashBytesUserName);
    				WTRKey keyUserName = new WTRKey("torusers", hashStringUserName);

            ctxt.write(keyUserName, usernames);
          }
          cookieInputStream.close();
          iStreamCookie.close();
        }
        pointInputStream.close();
        iStream.close();

    }
}
