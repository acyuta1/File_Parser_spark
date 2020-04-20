package com.acy.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileContent implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	String filename;
	Long linenum;
	String line;
	
	public String getFileName() {
		return filename;
	}

	public void setFileName(String fileName) {
		this.filename = fileName;
	}

	public Long getLinenum() {
		return linenum;
	}

	public void setLinenum(Long linenum) {
		this.linenum = linenum;
	}

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	public FileContent() {}
	
	public FileContent(String fileName, Long linenum, String line) {
		this.filename = fileName;
		this.linenum = linenum;
		this.line = line;
	}

	 public static List<String> columns() {
        List<String> columns = new ArrayList<String>();
        columns.add("filename");
        columns.add("linenum");
        columns.add("line"); 
        return columns;
    }
	 
	public String toString() {
		return getFileName() + " " + getLinenum() + " " + getLine();
	}
	

}
