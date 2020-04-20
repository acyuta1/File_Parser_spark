package com.acy.dao;

import java.io.Serializable;

import com.acy.model.FileContent;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;

import scala.collection.IndexedSeq;

public class FileContentReader extends GenericRowReader<FileContent>{
	
	private static final long serialVersionUID = 1L;
	private static RowReader<FileContent> reader = new FileContentReader();
	
	/**
	 * Reads the content of Cassandra table.
	 * @author achyu
	 *
	 */
	public static class FileContentRowReaderFactory implements RowReaderFactory<FileContent>, Serializable{
        private static final long serialVersionUID = 1L;
        
        /**
         * Will fetch rows from the cassandra table based on the column names provided and
         * returns the reader object.
         */
        @Override
        public RowReader<FileContent> rowReader(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
            return reader;
        }
        
        /**
         * Reference of the FileContent Class, which is what our rows will represent.
         */
        @Override
        public Class<FileContent> targetClass() {
            return FileContent.class;
        }
    }
	
	/**
	 * Every row will be converted into its corresponding FileContent objects and
	 * finally returns the fileContent object.
	 */
    @Override
    public FileContent read(Row row, String[] columnNames) {
        FileContent fileContent = new FileContent();        
        fileContent.setFileName(row.getString(0));
        fileContent.setLinenum(row.getLong(1));
        fileContent.setLine(row.getString(2));
        return fileContent;
    }


}
