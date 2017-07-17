/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.thebeastshop.batch.handler;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import com.thebeastshop.batch.annotation.AliasField;
import com.thebeastshop.batch.annotation.AliasTable;
import com.thebeastshop.batch.annotation.ExcludeField;
import com.thebeastshop.batch.context.HandlerContext;
import com.thebeastshop.batch.util.CamelCaseUtils;
import com.thebeastshop.batch.util.Reflections;

/**
 * 批插处理器
 */
public class DBRowBatchHandler<T> extends RowBatchHandler<T> {

	private JdbcTemplate jdbcTemplate;

	private String insertSql;

	private Field[] fields;

	// ================private==================
	private void prepareSql() {
		StringBuffer sql = new StringBuffer();
		Class<T> clazz = context.getClazz();
		sql.append(" insert into ").append(getAliasTable(clazz));
		sql.append("(");
		int m = 0;
		for (Field field : fields) {
			ExcludeField excludeField = field.getAnnotation(ExcludeField.class);
			if (excludeField == null) {
				m++;
				sql.append(getAliasField(field));
				sql.append(",");
			}
		}
		sql = sql.replace(sql.length() - 1, sql.length(), "");
		sql.append(") values(");
		for (int n = 0; n < m; n++) {
			sql.append("?");
			if (n < m - 1) {
				sql.append(",");
			}

		}
		sql.append(")");
		insertSql = sql.toString();
	}

	private String getAliasTable(Class<T> clazz) {
		AliasTable aliasTable = clazz.getAnnotation(AliasTable.class);
		if (aliasTable != null && StringUtils.isNotBlank(aliasTable.value())) {
			return aliasTable.value();
		} else {
			return CamelCaseUtils.toUnderScoreCase(clazz.getSimpleName());
		}
	}

	private String getAliasField(Field field) {
		AliasField aliasField = field.getAnnotation(AliasField.class);
		if (aliasField != null && StringUtils.isNotBlank(aliasField.value())) {
			return aliasField.value();
		} else {
			return CamelCaseUtils.toUnderScoreCase(field.getName());
		}
	}

	// ========================================

	public DBRowBatchHandler(HandlerContext<T> context, JdbcTemplate jdbcTemplate) {
		super(context);
		this.jdbcTemplate = jdbcTemplate;

		Class<T> clazz = context.getClazz();
		fields = clazz.getDeclaredFields();

		prepareSql();
	}

	@Override
	public void rowBatch(final List<T> batchList) {
		try {
			long startTimeMillis = System.currentTimeMillis();
			logger.info("开始批次插入数据库");
			if (batchList != null && batchList.size() > 0) {
				jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						T t = batchList.get(i);
						Object o = null;
						int n = 0;
						for (Field field : fields) {
							ExcludeField excludeField = field.getAnnotation(ExcludeField.class);
							if (excludeField == null) {
								n++;
								o = Reflections.invokeGetter(t, field.getName());
								if (o instanceof String) {
									ps.setString(n, (String) o);
								} else if (o instanceof byte[]) {
									ps.setBytes(n, (byte[]) o);
								} else if (o instanceof Short) {
									ps.setShort(n, (Short) o);
								} else if (o instanceof Integer) {
									ps.setInt(n, (Integer) o);
								} else if (o instanceof Long) {
									ps.setLong(n, (Long) o);
								} else if (o instanceof Date) {
									ps.setTimestamp(n, new Timestamp(((Date) o).getTime()));
								} else if (o instanceof BigDecimal) {
									ps.setBigDecimal(n, (BigDecimal) o);
								} else {
									ps.setNull(n, Types.NULL);
								}

							}
						}
					}

					@Override
					public int getBatchSize() {
						return batchList != null ? batchList.size() : 0;
					}
				});
				logger.info("this batch spend " + (System.currentTimeMillis() - startTimeMillis) + " millisecond");
			}
		} catch (Exception e) {
			logger.error("批次插入数据库异常: {}", e);
			if (exceptionCallback != null) {
				exceptionCallback.handle(batchList);
			}
		}
	}

}
