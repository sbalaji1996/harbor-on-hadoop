# harbor-on-hadoop

The first subtask identifies matching request/reply pairs (identical 4-tuples and occurring within 10 seconds of each other).

- QFDMatcherMapper: Create a new WebTrafficRecord from the input data and emit it along with a key that ensures that all matching request/reply pairs go to the same reducer.
- QFDMatcherReducer: Match up requests with replies, and emit them as RequestReplyMatch instances.

The second subtask takes these matches and constructs QueryFocusedDataSet objects that are then serialized to HDFS. 

- QFDWriterMapper.java: Emit each request/reply pair to the reducers so that it is written to QFDs based on srcIP, destIP, and cookie.
- QFDWriterReducer.java: Create a QueryFocuseDataSet from the request/reply matches and serialize the QFD object to a file on HDFS based on name and hash bytes.

The last group of files form a Hadoop job that collects data on Tor users. We can reuse the reducer class from the previous job, QFDWriterReducer, to write to the toruser QFD files.

- Take the input, a known IP address for a Tor exit node, and query the srcIP QFDs to find all cookies associated with any request originating from this address. To read the proper QFD files, you will need to use the hash of the srcIP.
- Using the cookies gathered from the previous step, query the QFDs to find all request/reply pairs associated with those cookies.
- Emit each match associated with the cookies, along with a WTRKey to organize matches by hash value, to the reducers, who will then write the toruser QFD files.