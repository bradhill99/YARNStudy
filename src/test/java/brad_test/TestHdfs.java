package brad_test;



import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestHdfs {
	public static final Log LOG = LogFactory.getLog(TestHdfs.class);
	
	private static String testString = "this is a test string";
	
	private static void createFile(FileSystem fs, Path f) throws IOException {
		FSDataOutputStream a_out = fs.create(f);
	    a_out.write(TestHdfs.testString.getBytes());
	    a_out.close();
	}
	
	@Test
	public void test() throws IOException {
		Configuration conf = new Configuration();
	    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nameNodeHttpPort(8888).build();
	    cluster.waitClusterUp();

	    FileSystem fs = cluster.getFileSystem();
	    LOG.info("Filesystem URI : " + fs.getUri());
	    LOG.info("Filesystem Home Directory : " + fs.getHomeDirectory());
	    LOG.info("Filesystem Working Directory : " + fs.getWorkingDirectory());
	    	    
	    Path dir = new Path("brad");
	    assertTrue(fs.mkdirs(dir));
        Path a = new Path(dir, "a");
        createFile(fs, a);
        
        // read data back
        // long length = fs.getFileStatus(a).getLen();
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(a)));
        String line;
        line=br.readLine();
        
        LOG.info("read line=" + line);
		assertTrue(line.compareTo(TestHdfs.testString) == 0);
	}

}
