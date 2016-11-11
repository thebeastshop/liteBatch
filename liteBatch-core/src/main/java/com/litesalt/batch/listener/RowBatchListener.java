/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.litesalt.batch.listener;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.entity.WrapItem;
import com.litesalt.batch.handler.RowBatchHandler;

/**
 * 批插监听管理器
 */
public class RowBatchListener<T> {
	
	private final static Logger log = Logger.getLogger(RowBatchListener.class);
	
	private RowBatchHandler<T> rowBatchHandler;
	
	
	public RowBatchListener(JdbcTemplate jdbcTemplate,int submitCapacity,Class<T> clazz){
		this.rowBatchHandler = new RowBatchHandler<T>(jdbcTemplate,submitCapacity, clazz);
		log.info("开始监听"+clazz.getSimpleName()+"的批次插入");
	}
	
	public void insertOneWithBatch(T t){
		if(t == null){
			log.warn("po must not be null!");
			return;
		}
		this.rowBatchHandler.insertWithBatch(new WrapItem<T>(t));
	}
	
	public void insertBatch(Collection<T> coll){
		for(T t : coll){
			this.rowBatchHandler.insertWithBatch(new WrapItem<T>(t));
		}
	}
	
	public void closeListener(){
		this.rowBatchHandler.shutDownHandler();
	}
	
	public void aliasTable(String tableName){
		this.rowBatchHandler.aliasTable(tableName);
	}
	
	public void aliasField(String fieldName,String columnName){
		this.rowBatchHandler.aliasField(fieldName, columnName);
	}
	
	public void addExcludeField(String fieldName){
		this.rowBatchHandler.addExcludeField(fieldName);
	}
}
