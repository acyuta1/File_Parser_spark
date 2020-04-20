package com.acy.dao;

import java.io.Serializable;

import com.acy.model.FileContent;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;

import scala.collection.IndexedSeq;
import scala.collection.Seq;

public class FileContentWriter implements RowWriter<FileContent>
{
    /**
	 * 
	 */
	private static final long serialVersionUID = -12889908475817543L;
	private static RowWriter<FileContent> writer = new FileContentWriter();
    
	/**
	 * Takes the class of RDD elements and 
	 * uses a default JavaBeanColumnMapper to map those elements to Cassandra rows
	 * @author achyu
	 *
	 */
    public static class FileContentRowWriterFactory implements RowWriterFactory<FileContent>, Serializable{
        private static final long serialVersionUID = 1L;
        @Override
        /**
         * Constructs a table of rows and from the sequence provided.
         * Returns the rowWriter object which will map itself to the actual Cassandra table.
         */
        public RowWriter<FileContent> rowWriter(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
        	
            return writer;
        }      
    }
    /**
     * This function maps the columns of cassandra
     */
    public Seq<String> columnNames() {       
        return scala.collection.JavaConversions.asScalaBuffer(FileContent.columns()).toList();
    }

    /**
     * Reads the content of a FileContent Object, stores the class members(the variables) in buffers.
     */
    public void readColumnValues(FileContent fileContent, Object[] buffer) {
        buffer[0] = fileContent.getFileName();
        buffer[1] = fileContent.getLinenum();
        buffer[2] = fileContent.getLine();    
    }


}