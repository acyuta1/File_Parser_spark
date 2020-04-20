package com.acy.FileParserSparkApp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.exceptions.CassandraException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acy.dao.FileContentReader.FileContentRowReaderFactory;
import com.acy.dao.FileContentWriter.FileContentRowWriterFactory;
import com.acy.model.FileContent;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

import scala.Tuple2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * Spark Application to read from text file and read/write to Cassandra database.
 * @author achyutha.mithra
 */
public class SparkFileParserApp implements Serializable {

	private static final long serialVersionUID = -5503469560409639528L;
	
	// variable modifier, will not be serialized.
	private transient SparkConf conf;
	private String keySpace = "fileparser";
	private String table = "filecontent";
	private FileContentRowWriterFactory fileContentWriter = new FileContentRowWriterFactory();
    private FileContentRowReaderFactory fileContentReader = new FileContentRowReaderFactory();
    private transient JavaSparkContext sc;
    Logger logger = LoggerFactory.getLogger(SparkFileParserApp.class);
    
	public SparkFileParserApp(SparkConf conf) {
        this.conf = conf;
    }
    
	public void initSpark() {
		sc = new JavaSparkContext(conf);
		logger.info("Spark Context created.");
	}
    /**
     * This function parses a text file, stores sentences and the corresponding sentence 
     * numbers in Cassandra Database.
     * @param fileName
     * @return 
     */
	public long parseAndStoreToCassandra(String fileName) {
			logger.info("parse process started.");
			JavaRDD<String> input = sc.textFile(fileName);
			
			// Splitting on the basis of occurence of ".", which means every sentence is separated.
		    JavaRDD<String> lines = input.flatMap(content -> Arrays.asList(content.split("\\.")));
		    
		    // Zipping every sentence along with its index, i.e., the sentence number.
		    JavaPairRDD<String, Long> tuples = lines.zipWithIndex();
		    
		    /*
		     * Transforming into required shape (The FileContent table) with
		     * columns FileName, LineNumber, Line.s
		     */
		    JavaRDD<FileContent> tuple1 = tuples.map(f->new FileContent(fileName, f._2, f._1));	 
		    logger.info("RDD of type FileContent created.");
		    Long count = tuples.count();
		    try {
		    	// Cassandra connector to store/save the content into cassandra database.
		    	logger.info("Inserting partitions into Cassandra Table");
				javaFunctions(tuple1).writerBuilder(keySpace, table, fileContentWriter).saveToCassandra();
			    } catch(CassandraException ex) {
			    	logger.warn("Cassandra Exception, please check if connected.");
			    	ex.printStackTrace();
			    }
		    logger.info("All rows stored in cassandra DB.");
		    sc.close();
		    return count;
	}
	
	/**
	 * This function retrieves a particular row from cassandra database,
	 * when provided with arguments fileName, LineNumber.
	 * @param fileName
	 * @param lineNum
	 * @return 
	 */
	public String retrieveRow(String fileName, Long lineNum) {
		logger.info("Retrieve row " + lineNum +  " from file "+ fileName);
		String row = null;
	      try {
	    /*
	     *  CassandraConnector to retrieve a particular row (in this case, based on 
	     *  the lineNumber provided.
	     */
	      CassandraTableScanJavaRDD<FileContent> fileContentRDD = javaFunctions(sc)
	    		  .cassandraTable(keySpace, table,fileContentReader)
	    		  .where("filename = ?", fileName)
	    		  .where("linenum = ?", lineNum);
	      
		      	try {
		      		// Printing the content thus obtained.
		      		logger.info(fileContentRDD.collect().get(0).toString());
		      		row = fileContentRDD.collect().get(0).toString();
		      	} catch(ArrayIndexOutOfBoundsException ex) {
		      		logger.warn("Array is empty, which indicates the absence of the requested file.");
		      		ex.printStackTrace();
		      	}
	      }	catch(CassandraException ex) {
	    	  logger.warn("Cassandra Exception, please check if connected.");
	    	  ex.printStackTrace();
	      }
	      sc.stop();
	      return row;
	}
	
	/**
	 * This function returns the content falling between the range startLine and endLine.
	 * @param fileName
	 * @param startLine
	 * @param endLine
	 */
	public List<Tuple2<String, FileContent>> retrieveBetweenRange(String fileName, Long startLine, Long endLine) {
		
		logger.info("Retrieve rows between range " + startLine +  " and "+ endLine +
				" from file " + fileName);
		JavaPairRDD<String, FileContent> fileContentRDD = null;
	      try {
	    /*
	     *  Cassandra Connector code to retrieve from cassandra table when provided
	     *  with a range.
	     */
	      fileContentRDD = javaFunctions(sc)
	    		  .cassandraTable(keySpace, table,fileContentReader)
	    		  .where("filename = ?", fileName)
	    		  .where("linenum >= ?", startLine)
	    		  .where("linenum <= ?", endLine)
	    		  .keyBy(new Function<FileContent, String>() {
					private static final long serialVersionUID = 1L;
					
					// Returns the string(overriden in FileContent) representation of entire row.
					public String call(FileContent fc) {
	    				  return fc.toString();
	    			  }
				});
	      try {
	    	  // Printing obtained rows.
		      fileContentRDD.foreach(f->logger.info(f.toString()));
		      } catch (ArrayIndexOutOfBoundsException ex) {
		    	  logger.warn("Array is empty, which indicates the absence of the requested file.");
		    	  ex.printStackTrace();
	      }
	      } catch(CassandraException ex) {
	    	  logger.warn("Cassandra Exception, please check if connected.");
	    	  ex.printStackTrace();
	      } 
	      List<Tuple2<String, FileContent>> rows = fileContentRDD.toArray();
	      sc.stop();
	      return rows;
	}
	
	/**
	 * This method retrieves all the rows of a particular cluster column (in 
	 * this case, the fileName).
	 * @param fileName
	 * @return 
	 */
	public List<FileContent> retrieveAllRowsOfFile(String fileName) {
		logger.info("Retrieving all rows from cassandra with fileName " + fileName);
		List<String> expectedOut = new ArrayList<String>();
		CassandraTableScanJavaRDD<FileContent> fileContentRDD = null;
	     try {
	    // Cassandra Connector code to retrieve all the rows.
	     fileContentRDD = javaFunctions(sc)
	    		 .cassandraTable(keySpace, table,fileContentReader)
	    		 .where("filename = ?", fileName);
		     try {
		    	 // Printing thus obtained rows.
		    	 fileContentRDD.foreach(f->
		    	 logger.info(f.toString()));
		    	
		     } catch(ArrayIndexOutOfBoundsException ex) {
		    	 logger.warn("Array is empty, which indicates the absence of the requested file.");
		    	 ex.printStackTrace();
		     }
	     } catch(CassandraException ex) {
	    	 logger.warn("Cassandra Exception, please check if connected.");
	    	 ex.printStackTrace();
	     }
	     List<FileContent> rows = fileContentRDD.toArray();
	     sc.stop();
	     return rows;
	}

}
