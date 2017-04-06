/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.thebeastshop.batch.entity;

/**
 * 数据列元信息
 */
public class DBColumnMetaData {
	
	private String columnName;
	
	private int dataType;
	
	private Object columnDef;
	
	public DBColumnMetaData(String columnName, int dataType, Object columnDef) {
		this.columnName = columnName;
		this.dataType = dataType;
		this.columnDef = columnDef;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public Object getColumnDef() {
		return columnDef;
	}

	public void setColumnDef(Object columnDef) {
		this.columnDef = columnDef;
	}
}
