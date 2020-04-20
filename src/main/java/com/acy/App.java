package com.acy;

import java.io.Serializable;

import org.apache.spark.SparkConf;

import com.acy.FileParserSparkApp.SparkFileParserApp;

public class App implements Serializable
{
	private static final long serialVersionUID = 1L;

	/**
	 * File Parser Application, with Cassandra Database!
	 * 
	 * @param args
	 * args[0] : Spark configuration, master.
	 * args[1] : Cassandra host ID
	 * args[2] : The method to execute. 
	 * 			1 - Parses a file and stores in cassandra database.
	 * 				args[3] - fileName
	 * 			2 - retrieves row from cassandra of a particular fileName and linenum
	 * 				args[3] - fileName
	 * 				args[4] - lineNum
	 * 			3 - retrieves rows between given range.
	 * 				args[3] - fileName
	 * 				args[4] - startLine
	 * 				args[5] - endLine
	 * 			4 - retrieves all the content from database of a particular file.
	 * 				args[3] - fileName
	 */
    public static void main( String[] args )
    {
    	/*
    	 * Spark Configurations. 
    	 */
    	SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster(args[0]);
        conf.set("spark.cassandra.connection.host", args[1]);
    	SparkFileParserApp app = new SparkFileParserApp(conf);
    	app.initSpark();
    	
    	String fileName; 
    	Long lineNum, startLine, endLine;
    	String choice = args[2];
    	
    	switch (choice) {
    	// To parse a file and stores its sentences in Cassandra Database.
		case "1":
			fileName = args[3];
			app.parseAndStoreToCassandra(fileName);
			break;
		// To retrieve a particular row belonging to a particular fileName.
		case "2":
			fileName = args[3];
			lineNum = Long.parseLong(args[4]);
			app.retrieveRow(fileName, lineNum);
			break;
		// To retrieve all the rows belonging to a particular fileName.
		case "3":
			fileName = args[3];
			startLine = Long.parseLong(args[4]);
			endLine = Long.parseLong(args[5]);
			app.retrieveBetweenRange(fileName, startLine, endLine);
			break;
		case "4":
			fileName = args[3];
			app.retrieveAllRowsOfFile(fileName);
			break;
		}
    }
}
