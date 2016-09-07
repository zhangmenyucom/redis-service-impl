package com.taylor.redis.serivce.impl.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface RedisCacheClean {

	/**
	 * 保存数据库
	 *
	 * @return
	 */
	public int DBIndex();

	/**
	 * key值
	 *
	 * @return
	 */
	public String[] key();

}
