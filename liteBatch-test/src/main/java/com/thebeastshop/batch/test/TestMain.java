package com.thebeastshop.batch.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
	
	@Resource(name="rowBatchListener1")
	private RowBatchListener<Person> rowBatchListener1;
	
	@Resource(name="rowBatchListener2")
	private RowBatchListener<Person> rowBatchListener2;
	
	@Resource(name="rowBatchListener3")
	private RowBatchListener<PersonVo> rowBatchListener3;

	@Test
	public void testBatch1() throws Exception {
		long start = System.currentTimeMillis();
		
		final String[] nameArr = new String[]{"荆轲","李白","成吉思汗","诸葛亮","曹操","赵云","黄忠","孙尚香","周瑜","刘备",
				"梦奇","吕布","太乙真人","东皇太一","牛魔","典韦","钟无艳","张飞","关羽","孙悟空","宫本武藏","亚瑟","艾琳"};
		
		final String[] titleArr = new String[]{"攻城狮","鼓励狮","占卜师","天文家","文学家"};
		
		final String[] companyArr = new String[]{"上海XXX信息科技有限公司","上海云海信息科技有限公司","北京金山网络科技有限公司","上海快钱支付清算有限公司","上海ABC网络科技有限公司"};
		
		try {
			Random random = new Random();
			Person person = null;
			for (int i = 0; i < 250300; i++) {
				person = new Person();
				person.setAge(random.nextInt(100));
				person.setAddress("XX马路"+random.nextInt(100)+"号");
				person.setName(nameArr[random.nextInt(nameArr.length)]);
				person.setCompany(companyArr[random.nextInt(companyArr.length)]);
				person.setEmail("abc"+random.nextInt(100)+"@163.com");
				person.setTitle(titleArr[random.nextInt(titleArr.length)]);
				person.setCreateTime(new Date());
				rowBatchListener1.insertOneWithBatch(person);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rowBatchListener1.flush();
		}
		long end = System.currentTimeMillis();
		System.out.println("耗时"+(end-start));
		System.in.read();
	}
	
	@Test
	public void testBatch2() throws Exception {
		long start = System.currentTimeMillis();
		
		try {
			List<Person> personList = new ArrayList<>();
			Random random = new Random();
			Person person = null;
			for (int i = 0; i < 40300; i++) {
				person = new Person();
				person.setAge(random.nextInt(100));
				person.setAddress("XX马路"+random.nextInt(100)+"号");
				person.setCompany("天天 向上科技有限公司");
				person.setName("张三");
				person.setCreateTime(new Date());
				personList.add(person);
			}
			rowBatchListener2.insertBatch(personList);
			rowBatchListener2.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("耗时"+(end-start));
		System.in.read();
	}

	@Test
	public void testBatch3() throws Exception {
		try {
			Random random = new Random();
			PersonVo personVo = null;
			for (int i = 0; i < 201000; i++) {
				personVo = new PersonVo();
				personVo.setPersonName("李四");
				personVo.setPersonAge(random.nextInt(100));
				personVo.setCoName("殖民地公司");
				personVo.setPersonAge(random.nextInt(100));
				personVo.setInsertDate(new Date());
				rowBatchListener3.insertOneWithBatch(personVo);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rowBatchListener3.flush();
		}

		System.in.read();
	}
}
