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
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.annotation.AliasField;
import com.litesalt.batch.annotation.AliasTable;
import com.litesalt.batch.annotation.ExcludeField;
import com.litesalt.batch.context.HandlerContext;
import com.litesalt.batch.entity.DBColumnMetaData;
import com.litesalt.batch.util.CamelCaseUtils;
import com.litesalt.batch.util.Reflections;

/**
 * 批插处理器
 */
public class DBRowBatchHandler<T> extends RowBatchHandler<T> {

	private JdbcTemplate jdbcTemplate;

	private String insertSql;

	private Field[] fields;

	private Map<String, DBColumnMetaData> metaMap = new HashMap<String, DBColumnMetaData>();

	// ================private==================
	private void prepareSql() {
		StringBuffer sql = new StringBuffer();
		Class<T> clazz = context.getClazz();
		sql.append(" insert into ").append(getAliasTable(clazz));
		sql.append("(");
		int m = 0;
		for (Field field : clazz.getDeclaredFields()) {
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

	private boolean initDBMetaData() {
		boolean flag = false;
		try {
			Class<T> clazz = context.getClazz();
			DatabaseMetaData metaData = jdbcTemplate.getDataSource().getConnection().getMetaData();
			ResultSet rs = metaData.getColumns(null, "%", getAliasTable(clazz), "%");
			while (rs.next()) {
				metaMap.put(rs.getString("COLUMN_NAME"), new DBColumnMetaData(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"), rs.getObject("COLUMN_DEF")));
			}
			flag = true;
		} catch (Exception e) {
			logger.error("init db metadata wrong", e);
			flag = false;
		}
		return flag;
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

		if (initDBMetaData()) {
			Class<T> clazz = context.getClazz();
			fields = clazz.getDeclaredFields();

			prepareSql();
		}
	}

	@Override
	public void rowBatch(final List<T> batchList) {
		long startTimeMillis = System.currentTimeMillis();
		logger.info("开始批次插入数据库");
		if (batchList != null && batchList.size() > 0) {
			jdbcTemplate.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					T t = batchList.get(i);
					Object o = null;
					DBColumnMetaData metaData = null;
					int n = 0;
					for (Field field : fields) {
						ExcludeField excludeField = field.getAnnotation(ExcludeField.class);
						if (excludeField == null) {
							n++;
							o = Reflections.invokeGetter(t, field.getName());
							metaData = metaMap.get(getAliasField(field));

							// 如果值为null，还要看默认值，如果有默认值，取元数据中的默认值
							if (o == null) {
								if (metaData.getColumnDef() == null) {
									ps.setNull(n, Types.NULL);
									continue;
								}
								o = metaData.getColumnDef();
							}

							switch (metaData.getDataType()) {
							case Types.CHAR:
							case Types.VARCHAR:
							case Types.NVARCHAR:
								ps.setString(n, (String) o);
								break;
							case Types.BLOB:
							case Types.LONGVARBINARY:
								ps.setBytes(n, (byte[]) o);
								break;
							case Types.TINYINT:
							case Types.SMALLINT:
								ps.setShort(n, Short.parseShort(o.toString()));
								break;
							case Types.INTEGER:
								ps.setInt(n, Integer.parseInt(o.toString()));
								break;
							case Types.BIGINT:
								ps.setLong(n, Long.parseLong(o.toString()));
								break;
							case Types.DATE:
								ps.setDate(n, new java.sql.Date(((Date) o).getTime()));
								break;
							case Types.TIMESTAMP:
								ps.setTimestamp(n, new Timestamp(((Date) o).getTime()));
								break;
							case Types.DECIMAL:
								ps.setBigDecimal(n, (BigDecimal) o);
								break;
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
	}

}
