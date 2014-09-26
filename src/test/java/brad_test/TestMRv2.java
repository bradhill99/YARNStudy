package brad_test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.TestMRJobs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMRv2 {
	public static final Log LOG = LogFactory.getLog(TestHdfs.class);
	
	  private static Path inDir = null;
	  private static Path outDir = null;
	  private static Path testdir = null;
	  private static Path[] inFiles = new Path[5];

	  protected static MiniMRYarnCluster mrCluster;
	  protected static MiniDFSCluster dfsCluster;

	  private static Configuration conf = new Configuration();
	  private static FileSystem localFs;
	  private static FileSystem remoteFs;
	  
	  private static String testString = "this is a test string";

	  static {
	    try {
	      localFs = FileSystem.getLocal(conf);
	    } catch (IOException io) {
	      throw new RuntimeException("problem getting local fs", io);
	    }
	  }
	  
	  private static Path TEST_ROOT_DIR = new Path("target",
		      TestMRJobs.class.getName() + "-tmpDir").makeQualified(localFs);
		  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
		  private static final String OUTPUT_ROOT_DIR = "/tmp/" +
		    TestMRJobs.class.getSimpleName();
		  
	  private class InternalClass {
	  
	  }
	  
		private static void createFile(FileSystem fs, Path f) throws IOException {
			FSDataOutputStream a_out = fs.create(f);
		    a_out.write(TestMRv2.testString.getBytes());
		    a_out.close();
		}
		
	  public static RunningJob createJob() throws IOException {
		    //final Job baseJob = new Job(mrCluster.getConfig());
		  final JobConf conf = new JobConf();
		    conf.setJobName("wordcount");
		    conf.setInputFormat(TextInputFormat.class);
		    
		    // the keys are words (strings)
		    conf.setOutputKeyClass(Text.class);
		    // the values are counts (ints)
		    conf.setOutputValueClass(IntWritable.class);
		    
		    conf.setMapperClass(WordCount.MapClass.class);        
		    conf.setCombinerClass(WordCount.Reduce.class);
		    conf.setReducerClass(WordCount.Reduce.class);
		    
		    Path wordCountInDir = new Path(localFs.getWorkingDirectory(), "src/test/resources/");
		    FileInputFormat.setInputPaths(conf, wordCountInDir);
		    FileOutputFormat.setOutputPath(conf, outDir);
		    conf.setNumMapTasks(1);
		    conf.setNumReduceTasks(1);
		    
		    JobClient jobClient = new JobClient(conf);
		    return jobClient.runJob(conf);
	  }
		

	  public void getOutput(FileSystem fs, Path path) throws IOException {
		  //FSDataInputStream inputStream = fs.open(new Path(path, "part-r-00000"));
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(path, "part-00000"))));
	        
	        StringBuffer lineBuffer = new StringBuffer();	        
	        String line = br.readLine();
	        while(line != null) {
	        	lineBuffer.append(line + "\n");	        	
	        	line=br.readLine();
	        }
	        LOG.info(lineBuffer);
	        br.close();
		  }
		    
	  @BeforeClass
	  public static void setup() throws IOException {
		LOG.info("Brad:" + MiniMRYarnCluster.APPJAR);	    
		dfsCluster = new MiniDFSCluster.Builder(conf).nameNodeHttpPort(8888).build();
		dfsCluster.waitClusterUp();
		remoteFs = dfsCluster.getFileSystem();
   
	    LOG.info("remote Filesystem URI : " + remoteFs.getUri());
	    LOG.info("remote Filesystem Home Directory : " + remoteFs.getHomeDirectory());
	    LOG.info("remote Filesystem Working Directory : " + remoteFs.getWorkingDirectory());

	    LOG.info("local Filesystem URI : " + localFs.getUri());
	    LOG.info("local Filesystem Home Directory : " + localFs.getHomeDirectory());
	    LOG.info("local Filesystem Working Directory : " + localFs.getWorkingDirectory());

	    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
	        LOG.info("Brad:MRAppJar " + MiniMRYarnCluster.APPJAR
	                 + " not found. Not running test.");
	        return;
	    }
	    
	    //final Path TEST_ROOT_DIR = new Path(System.getProperty("test.build.data", "/tmp"));
	    final Path TEST_ROOT_DIR = new Path("brad");
	    testdir = new Path(TEST_ROOT_DIR, "TestMiniMRClientCluster");
	    inDir = new Path(testdir, "in");
	    outDir = new Path(testdir, "out");
	    
	    if (remoteFs.exists(testdir) && !remoteFs.delete(testdir, true)) {
	      throw new IOException("Could not delete " + testdir);
	    }
	    /*if (!remoteFs.mkdirs(inDir)) {
	      throw new IOException("Mkdirs failed to create " + inDir);
	    }*/
	    if (remoteFs.exists(outDir) && !remoteFs.delete(outDir, true)) {
		      throw new IOException("Could not delete " + outDir);
	    }
	    if (localFs.exists(outDir) && !localFs.delete(outDir, true)) {
		      throw new IOException("Could not delete " + outDir);
	    }
	    
	    for (int i = 0; i < inFiles.length; i++) {
	      inFiles[i] = new Path(inDir, "part_" + i);
	      createFile(remoteFs, inFiles[i]);
	    }

	    // create the mini cluster to be used for the tests
	    if (mrCluster == null) {
	        mrCluster = new MiniMRYarnCluster(TestMRv2.class.getName(), 1);
	        
	        conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
	        conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
	        mrCluster.init(conf);
	        mrCluster.start();
	    }
	    
	    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
	    // workaround the absent public discache.
	    localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
	    localFs.setPermission(APP_JAR, new FsPermission("700"));
	  }	

	  @AfterClass
	  public static void tearDown() {
	    if (mrCluster != null) {
	      mrCluster.stop();
	      mrCluster = null;
	    }
	    if (dfsCluster != null) {
	      dfsCluster.shutdown();
	      dfsCluster = null;
	    }
	  }
	  
/*	@Test
	public void test() throws IOException, ClassNotFoundException, InterruptedException {
		final Job job = createJob();
		FileInputFormat.addInputPath(job, inDir);
		FileOutputFormat.setOutputPath(job, outDir);
	    //org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, inDir);
	    //org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(outDir, "testJob"));
	    assertTrue(job.waitForCompletion(true));
	    
	    getOutput(FileSystem.getLocal(new Configuration()), outDir);
		fail("Not yet implemented");
	}*/
	
	@Test
	public void testWordCount() throws IOException, ClassNotFoundException, InterruptedException {
		RunningJob job = createJob();
		LOG.info("job status=" + job.isSuccessful());
		//LOG.info(MapReduceTestUtil.readOutput(outDir, localFs.getConf()));
		getOutput(localFs, outDir);
	    
	    //getOutput(FileSystem.getLocal(new Configuration()), outDir);
		assertTrue(job.isSuccessful() == true);
	}
}
