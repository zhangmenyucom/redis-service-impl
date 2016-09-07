package com.taylor.redis.common.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.taylor.redis.service.RedisClientService;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:META-INF/spring/spring-application.xml", "classpath:META-INF/spring/spring-servlet.xml" })
public class TestRedisService {

	@Autowired
	@Qualifier("redisSimpleClient")
	private RedisClientService redisClientService;

	@Test
	public void testSendMessage() {
		// redisClientService.set("test",
		// "123+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		System.out.println(redisClientService.get("test"));
	}
}
