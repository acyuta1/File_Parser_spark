package com.acy.spark.FileParserSparkApp;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.JUnit4;

import com.acy.FileParserSparkApp.SparkFileParserApp;
import com.acy.dao.FileContentReader.FileContentRowReaderFactory;
import com.acy.dao.FileContentWriter.FileContentRowWriterFactory;
import com.acy.model.FileContent;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

import scala.Tuple2;


public class SparkFileParserAppTest {
    private static SparkFileParserApp sparkApp;
    private static JavaSparkContext sc;
    private FileContentRowWriterFactory fileContentWriter = new FileContentRowWriterFactory();
    private FileContentRowReaderFactory fileContentReader = new FileContentRowReaderFactory();



	@Before
	public void init() {
		System.setProperty("hadoop.home.dir", "C:\\Applications\\Environments\\winutils");
    	
    	/*
    	 * Spark Configurations. 
    	 */
    	SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("local[*]");
        conf.set("spark.cassandra.connection.host", "127.0.0.1");
//    	sc = new JavaSparkContext(conf);
    	sparkApp = new SparkFileParserApp(conf);
    	sparkApp.initSpark();
	}
	
	@Test
	public void when_parse_correct_count_to_be_returned() {
		Long expectedCount = 3L; 
		Long a = sparkApp.parseAndStoreToCassandra("test.txt");
	    assertEquals(expectedCount, a);
	}
	

	@Test
	public void test_for_retrieving_a_row() {
		String expectedOutput = new FileContent("test.txt",0L,"Paragaph line1").toString();
		String observedOutput = sparkApp.retrieveRow("test.txt", 0L);
		assertEquals(expectedOutput, observedOutput);
	}
	
	@Test
	public void test_for_retrieving_rows_between_range() {
		List<String> expectedOut = new ArrayList<String>();
		expectedOut.add("test.txt 0 Paragaph line1");
		expectedOut.add("test.txt 1  line2");
		List<Tuple2<String, FileContent>> observedOutput = sparkApp.retrieveBetweenRange("test.txt", 0L, 1L);
		assertEquals(expectedOut.get(0), observedOutput.get(0)._2.toString());
		assertEquals(expectedOut.size(), observedOutput.size());
	}

	@Test
	public void test_for_retrieving_all_rows() {
		List<String> expectedOut = new ArrayList<String>();
		expectedOut.add("test.txt 0 Paragaph line1");
		expectedOut.add("test.txt 1  line2");
		expectedOut.add("test.txt 2 Paragraph lin3");
		List<FileContent> observedOutput = sparkApp.retrieveAllRowsOfFile("test.txt");
		assertEquals(expectedOut.get(0), observedOutput.get(0).toString());
		assertEquals(expectedOut.size(), observedOutput.size());
	}

	
	
}
