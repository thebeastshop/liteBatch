/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.litesalt.batch.handler;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import com.litesalt.batch.entity.WrapItem;
import com.litesalt.batch.util.CamelCaseUtils;
import com.litesalt.batch.util.Reflections;

/**
 * 批插处理器
 */
public class RowBatchHandler<T> {
	
	private final Logger logger = Logger.getLogger(RowBatchHandler.class);
	
	private JdbcTemplate jdbcTemplate;
	
	private BlockingQueue<WrapItem<T>> queue;
	
	private Class<T> clazz;
	
	private String insertSql;
	
	private int queueCapacity = 1024*1024;
	
	private int submitCapacity;
	
	private Thread rowBatchThread;
	
	private Field[] fields;
	
	private long startTimeMillis;
	
	private Map<String, String> aliasMap;
	
	private List<String> excludeField = new ArrayList<String>();
	
	public RowBatchHandler(JdbcTemplate jdbcTemplate,int submitCapacity,Class<T> clazz) {
		this.jdbcTemplate = jdbcTemplate;
		this.queue = new LinkedBlockingQueue<WrapItem<T>>(queueCapacity);
		this.submitCapacity = submitCapacity;
		this.clazz = clazz;
		fields = clazz.getDeclaredFields();
		excludeField.add("id");
		excludeField.add("serialVersionUID");
		
		rowBatchThread = new Thread(){
			public void run() {
				new RowDeal().deal();
			};
		};
		rowBatchThread.start();
		
		startTimeMillis = System.currentTimeMillis();
	}
	
	public void insertWithBatch(WrapItem<T> item){
		try {
			if(StringUtils.isEmpty(this.insertSql)){
				this.prepareSql();
			}
			queue.put(item);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public WrapItem<T> take(){
		try {
			return queue.take();
		} catch (InterruptedException e) {
			logger.error("take is interrupted", e);
			return null;
		}
	}
	
	public Class<T> getClazz(){
		return clazz;
	}
	
	private class RowDeal{
		
		private final Logger log = Logger.getLogger(RowDeal.class);
		
		private final List<T> batchList = new ArrayList<T>();
		
		public void deal(){
			while(true){
				try{
					WrapItem<T> wrapItem = take();
					//如果从队列里取到的值为关闭监听的信号，则做完剩余的缓冲List里的数据，线程退出
					if(wrapItem.isShutdownSignature()){
						rowBatch();
						break;
					}
					batchList.add(wrapItem.getT());
					if(batchList.size() >= submitCapacity){
						rowBatch();
					}
				}catch(Exception e){
					log.error("批次插入发生异常", e);
					break;
				}
			}
			logger.info("this batch spend " + (System.currentTimeMillis()-startTimeMillis) + " millisecond");
			log.info("linstenr is shut down!");
		}
		
		private void rowBatch() throws Exception{
			log.info("开始批次插入");
			jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					T t = batchList.get(i);
					Object o = null;
					int n = 0;
					for(Field field : fields){
						if(!excludeField.contains(field.getName())){
							n++;
							o = Reflections.invokeGetter(t, field.getName());
							if(o == null){
								ps.setNull(n, Types.NULL);
							}else if(o instanceof String){
								ps.setString(n,(String)o);
							}else if(o instanceof Long){
								ps.setLong(n, (Long)o);
							}else if(o instanceof Integer){
								ps.setInt(n, (Integer)o);
							}else if(o instanceof Short){
								ps.setShort(n, (Short)o);
							}else if(o instanceof Date){
								ps.setTimestamp(n, new Timestamp(((Date)o).getTime()));
							}
						}
					}
				}
				
				@Override
				public int getBatchSize() {
					return batchList.size();
				}
			});
			batchList.clear();
		}
	}
	
	private void prepareSql(){
		StringBuffer sql = new StringBuffer();
		sql.append(" insert into ").append(getAliasTable(this.clazz.getSimpleName()));
		sql.append("(");
		int i = 0;
		int m = 0;
		for(Field field : this.clazz.getDeclaredFields()){
			i++;
			if(!this.excludeField.contains(field.getName())){
				m++;
				sql.append(getAliasField(field.getName()));
				if(i < this.clazz.getDeclaredFields().length){
					sql.append(",");
				}
			}
		}
		sql.append(") values(");
		for(int n = 0;n<m; n++){
			sql.append("?");
			if(n<m-1){
				sql.append(",");
			}
			
		}
		sql.append(")");
		this.insertSql = sql.toString();
	}
	
	public void shutDownHandler(){
		try {
			//往队列插入一个关闭的信号，队列处理监听到关闭信号会退出监听
			WrapItem<T> wrapItem = new WrapItem<T>();
			wrapItem.setShutdownSignature(true);
			this.queue.put(wrapItem);
		} catch (InterruptedException e) {
			logger.error("shut down cause error",e);
		}
	}
	
	private String getAliasTable(String poName){
		if(this.aliasMap!= null && this.aliasMap.containsKey("TABLE")){
			return aliasMap.get("TABLE");
		}else{
			return CamelCaseUtils.toUnderScoreCase(poName);
		}
	}
	
	private String getAliasField(String fieldName){
		if(this.aliasMap!= null && this.aliasMap.containsKey(fieldName)){
			return aliasMap.get(fieldName);
		}else{
			return CamelCaseUtils.toUnderScoreCase(fieldName);
		}
	}
	
	public void aliasTable(String tableName){
		if(this.aliasMap == null){
			this.aliasMap = new HashMap<String, String>();
		}
		this.aliasMap.put("TABLE", tableName);
	}
	
	public void aliasField(String fieldName,String columnName){
		if(this.aliasMap == null){
			this.aliasMap = new HashMap<String, String>();
		}
		this.aliasMap.put(fieldName, columnName);
	}
	
	public void addExcludeField(String fieldName){
		this.excludeField.add(fieldName);
	}
}
