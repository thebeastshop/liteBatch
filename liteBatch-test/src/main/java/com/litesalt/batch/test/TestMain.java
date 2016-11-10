package com.litesalt.batch.test;

import java.util.Date;
import java.util.Random;
import javax.annotation.Resource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import com.litesalt.batch.listener.RowBatchListener;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring-db.xml"}) 
public class TestMain {
	@Resource
	private JdbcTemplate jdbcTemplate;
	
	@Test
	public void testBatch() throws Exception{
		RowBatchListener<Person> rowBatchListener = new RowBatchListener<>(jdbcTemplate, 5000, Person.class);
		try{
			Random random = new Random();
			Person person = null;
			for(int i=0;i<286000;i++){
				person = new Person();
				person.setAge(random.nextInt(100));
				person.setAddress("XX马路1号");
				person.setCompany("天天 向上科技有限公司");
				person.setName("张三");
				person.setCreateTime(new Date());
				rowBatchListener.insertWithBatch(person);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			rowBatchListener.closeListener();
		}
		
		System.in.read();
	}
}
