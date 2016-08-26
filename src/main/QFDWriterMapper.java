import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;

public class QFDWriterMapper extends Mapper<RequestReplyMatch, NullWritable, WTRKey, RequestReplyMatch> {
    private MessageDigest messageDigest;


    // For the Query Focused Dataset, we take the key we are using,
    // hash it with a cryptographic hash function, and only use
    // the HashUtils.NUM_HASH_BYTES.

    // We start by creating a messageDigest instance, and preseed it
    // with the salt also contained in HashUtils.  This is so that
    // nobody can predict which names map to which locations
    @Override
    public void setup(Context ctxt) throws IOException, InterruptedException {
        super.setup(ctxt);
        try {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is required on all Java platforms, so this
            // should never occur
            throw new RuntimeException("SHA-1 algorithm not available");
        }
        // Now we are adding the salt...
        messageDigest.update(HashUtils.SALT.getBytes(StandardCharsets.UTF_8));
    }

    @Override
	public void map(RequestReplyMatch record, NullWritable ignore, Context ctxt)
	        throws IOException, InterruptedException {

        // This is the key mapping function.  You should
        // have the outputs be WTRKey/RequestReplyMatch pairs
        // since this dictates how to go to a particular
        // QFD

        // As an example, to dispatch for the "srcIP", the
        // hash should be

            // MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
            // md.update(record.getSrcIp().getBytes(StandardCharsets.UTF_8));
            // byte[] hash = md.digest();
            // byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            // String hashString = DatatypeConverter.printHexBinary(hashBytes);
            // WTRKey key = new WTRKey("srcIP", hashString);
            // ctxt.write(key, record);

        // You need to do srcIP, destIP, and cookie QFD dispatch, and should
        // be able to use a much more generic structure

				//Emit each request/reply pair to the reducers so that it is written to QFDs based on srcIP, destIP, and cookie.

        //System.err.println("Here needs implementation!");

				MessageDigest mdSRC = HashUtils.cloneMessageDigest(messageDigest);
				mdSRC.update(record.getSrcIp().getBytes(StandardCharsets.UTF_8));
				byte[] hashSRC = mdSRC.digest();
				byte[] hashBytesSRC = Arrays.copyOf(hashSRC, HashUtils.NUM_HASH_BYTES);
				String hashStringSRC = DatatypeConverter.printHexBinary(hashBytesSRC);
				WTRKey keySRC = new WTRKey("srcIP", hashStringSRC);
				ctxt.write(keySRC, record);

				MessageDigest mdDEST = HashUtils.cloneMessageDigest(messageDigest);
				mdDEST.update(record.getDestIp().getBytes(StandardCharsets.UTF_8));
				byte[] hashDEST = mdDEST.digest();
				byte[] hashBytesDEST = Arrays.copyOf(hashDEST, HashUtils.NUM_HASH_BYTES);
				String hashStringDEST = DatatypeConverter.printHexBinary(hashBytesDEST);
				WTRKey keyDEST = new WTRKey("destIP", hashStringDEST);
				ctxt.write(keyDEST, record);

				MessageDigest mdCookie = HashUtils.cloneMessageDigest(messageDigest);
				mdCookie.update(record.getCookie().getBytes(StandardCharsets.UTF_8));
				byte[] hashCookie = mdCookie.digest();
				byte[] hashBytesCookie = Arrays.copyOf(hashCookie, HashUtils.NUM_HASH_BYTES);
				String hashStringCookie = DatatypeConverter.printHexBinary(hashBytesCookie);
				WTRKey keyCookie = new WTRKey("cookie", hashStringCookie);
				ctxt.write(keyCookie, record);
    }

}
