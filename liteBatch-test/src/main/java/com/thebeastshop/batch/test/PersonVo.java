/**
 * <p>Title: litePromise</p>
 * <p>Description: litePromise,give you a promise</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-11
 * @version 1.0
 */
package com.thebeastshop.batch.test;

import java.util.Date;

import com.thebeastshop.batch.annotation.AliasField;
import com.thebeastshop.batch.annotation.AliasTable;
import com.thebeastshop.batch.annotation.ExcludeField;


@AliasTable("person")
public class PersonVo {
	@AliasField("name")
	private String personName;
	@AliasField("age")
	private Integer personAge;
	@ExcludeField
	private String coName;
	@AliasField("create_time")
	private Date insertDate;
	public String getPersonName() {
		return personName;
	}
	public void setPersonName(String personName) {
		this.personName = personName;
	}
	public Integer getPersonAge() {
		return personAge;
	}
	public void setPersonAge(Integer personAge) {
		this.personAge = personAge;
	}
	public String getCoName() {
		return coName;
	}
	public void setCoName(String coName) {
		this.coName = coName;
	}
	public Date getInsertDate() {
		return insertDate;
	}
	public void setInsertDate(Date insertDate) {
		this.insertDate = insertDate;
	}
}
