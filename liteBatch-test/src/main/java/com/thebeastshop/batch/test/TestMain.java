package com.thebeastshop.batch.test;

import java.util.Date;
import java.util.Random;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.thebeastshop.batch.DBRowBatchListenerBuilder;
import com.thebeastshop.batch.listener.RowBatchListener;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring-db.xml" })
public class TestMain {
	@Resource
	private JdbcTemplate jdbcTemplate;
	
	@Resource
	private RowBatchListener<Person> rowBatchListener;

	@Test
	public void testBatch1() throws Exception {
		long start = System.currentTimeMillis();
		
		try {
			Random random = new Random();
			Person person = null;
			for (int i = 0; i < 100300; i++) {
				person = new Person();
				person.setAge(random.nextInt(100));
				person.setAddress("XX马路"+random.nextInt(100)+"号");
				person.setCompany("天天 向上科技有限公司");
				person.setName("张三");
				person.setCreateTime(new Date());
				rowBatchListener.insertOneWithBatch(person);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rowBatchListener.flush();
		}
		long end = System.currentTimeMillis();
		System.out.println("耗时"+(end-start));
		System.in.read();
	}

	@Test
	public void testBatch2() throws Exception {
		RowBatchListener<PersonVo> rowBatchListener = DBRowBatchListenerBuilder.buildMemoryRowBatchListener(jdbcTemplate, 5000, PersonVo.class);
		try {
			Random random = new Random();
			PersonVo personVo = null;
			for (int i = 0; i < 286000; i++) {
				personVo = new PersonVo();
				personVo.setPersonName("李四");
				personVo.setPersonAge(random.nextInt(100));
				personVo.setCoName("XX公司");
				rowBatchListener.insertOneWithBatch(personVo);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}

		System.in.read();
	}
}
