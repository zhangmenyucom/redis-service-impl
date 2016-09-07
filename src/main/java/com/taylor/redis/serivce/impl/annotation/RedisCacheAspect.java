package com.taylor.redis.serivce.impl.annotation;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.LocalVariableTableParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taylor.common.jackson.JacksonUtil;
import com.taylor.redis.service.impl.RedisSimpleClient;

import lombok.extern.log4j.Log4j2;

@Aspect
@Component
@Log4j2
public class RedisCacheAspect {

	private static final int ONEDAY = 60 * 60 * 24; // 24h

	@Autowired
	@Qualifier(value = "weimobSimpleRedisClient")
	private RedisSimpleClient weimobRedisSimpleClient;

	@Around("@annotation(RedisCacheGet)")
	public Object cacheGet(ProceedingJoinPoint joinPoint) throws Throwable {
		MethodSignature signature = (MethodSignature) joinPoint.getSignature();
		Method method = signature.getMethod();
		Object[] args = joinPoint.getArgs();
		return goRedisCacheGet(args, method, joinPoint);

	}

	@Around("@annotation(RedisCacheClean)")
	public Object cacheClean(ProceedingJoinPoint joinPoint) throws Throwable {
		MethodSignature signature = (MethodSignature) joinPoint.getSignature();
		Method method = signature.getMethod();
		Object[] args = joinPoint.getArgs();
		return goRedisCacheClean(args, method, joinPoint);

	}

	/**
	 * @param args
	 * @param method
	 * @param joinPoint
	 * @return
	 * @throws Throwable
	 * @description redisCacheGet注解解析方法
	 */
	public Object goRedisCacheGet(Object[] args, Method method, ProceedingJoinPoint joinPoint) throws Throwable {

		ExpressionParser parser = new SpelExpressionParser();
		EvaluationContext context = new StandardEvaluationContext();

		// 获取被拦截方法参数名列表(使用Spring支持类库)
		LocalVariableTableParameterNameDiscoverer u = new LocalVariableTableParameterNameDiscoverer();
		String[] paraNameArr = u.getParameterNames(method);
		// 把方法参数放入SPEL上下文中
		for (int i = 0; i < paraNameArr.length; i++) {
			context.setVariable(paraNameArr[i], args[i]);
		}
		// 如果有这个注解，则获取注解类
		RedisCacheGet methodType = method.getAnnotation(RedisCacheGet.class);
		String key = parser.parseExpression(methodType.key()).getValue(context, String.class);
		if (methodType.dataType() == RedisCacheGet.DataType.JSON) {// JSON形式保存
			if (methodType.force() == true) {// 强制更新数据
				Object object = joinPoint.proceed(args);
				if (object != null) {
					setRedisValueJson(methodType, key, object);
				}
				// 返回值类型
				return object;
			} else {
				if (weimobRedisSimpleClient.exists(methodType.DBIndex(), key)) {
					String json = weimobRedisSimpleClient.get(methodType.DBIndex(), key);

					try {
						log.debug("redis" + method.getReturnType() + "key{}" + key + "\t value{}" + json);
						if (method.getGenericReturnType().toString().contains("List") || method.getGenericReturnType().toString().contains("Set") || method.getGenericReturnType().toString().contains("Map")) {
							ObjectMapper mapper = JacksonUtil.getInstance().getObjectMapper();
							String clazzName = ((ParameterizedType) method.getGenericReturnType()).getActualTypeArguments()[0].toString().substring(6);
							Object o = Class.forName(clazzName).newInstance();
							JavaType javaType = getCollectionType(mapper, method.getReturnType(), o.getClass());
							return JacksonUtil.getInstance().json2JavaType(json, javaType);
						} else {
							return JacksonUtil.getInstance().json2Bean(json, method.getReturnType());
						}
					} catch (Exception e) {
						log.error(e.getMessage());
					}
					return null;
				} else {// 查询数据，缓存，返回对象
					Object object = joinPoint.proceed(args);
					if (object != null) {
						setRedisValueJson(methodType, key, object);
					}
					return object;
				}
			}
		} else {// CLASS形式保存
			if (methodType.force() == true) {// 强制更新数据
				Object object = joinPoint.proceed(args);
				if (object != null) {
					setRedisValueClass(methodType, key, object);
				}
				// 返回值类型
				return object;
			} else {
				if (weimobRedisSimpleClient.exists(methodType.DBIndex(), key)) {// 对象存在直接返回
					return weimobRedisSimpleClient.getObject(methodType.DBIndex(), key);
				} else {// 查询数据，缓存，返回对象
					Object object = joinPoint.proceed(args);
					if (object != null) {
						setRedisValueClass(methodType, key, object);
					}
					return object;
				}
			}
		}
	}

	/**
	 * @param methodType
	 * @param key
	 * @param object
	 */
	private void setRedisValueClass(RedisCacheGet methodType, String key, Object object) {
		// 设置缓存时长
		if (methodType.expire() == 0) {
			weimobRedisSimpleClient.set(methodType.DBIndex(), key, object);
		} else if (methodType.expire() == 1) {
			weimobRedisSimpleClient.set(methodType.DBIndex(), key, ONEDAY, object);
		} else {
			weimobRedisSimpleClient.set(methodType.DBIndex(), key, methodType.expire(), object);
		}
	}

	/**
	 * @param methodType
	 * @param key
	 * @param object
	 */
	private void setRedisValueJson(RedisCacheGet methodType, String key, Object object) {

		if (methodType.expire() == 0) {// 0:永不过期
			weimobRedisSimpleClient.set(methodType.DBIndex(), key, JSON.toJSONString(object));
		} else if (methodType.expire() == 1) {// 1:过期时间为24h
			weimobRedisSimpleClient.set(methodType.DBIndex(), key, ONEDAY, JSON.toJSONString(object));
		} else {// 手动指定
			weimobRedisSimpleClient.set(methodType.DBIndex(), key, methodType.expire(), JSON.toJSONString(object));
		}

	}

	/**
	 * @param args
	 * @param method
	 * @param joinPoint
	 * @return
	 * @throws Throwable
	 * @description redisCache 清除方法
	 */
	public Object goRedisCacheClean(Object[] args, Method method, ProceedingJoinPoint joinPoint) throws Throwable {
		ExpressionParser parser = new SpelExpressionParser();
		EvaluationContext context = new StandardEvaluationContext();

		// 获取被拦截方法参数名列表(使用Spring支持类库)
		LocalVariableTableParameterNameDiscoverer u = new LocalVariableTableParameterNameDiscoverer();
		String[] paraNameArr = u.getParameterNames(method);
		// 把方法参数放入SPEL上下文中
		for (int i = 0; i < paraNameArr.length; i++) {
			context.setVariable(paraNameArr[i], args[i]);
		}
		// 如果有这个注解，则获取注解类
		Object object = joinPoint.proceed(args);

		// 如果有这个注解，则获取注解类
		RedisCacheClean methodType = method.getAnnotation(RedisCacheClean.class);
		for (String str : methodType.key()) {
			String key = parser.parseExpression(str).getValue(context, String.class);
			weimobRedisSimpleClient.del(methodType.DBIndex(), key);
		}
		return object;
	}

	private static JavaType getCollectionType(ObjectMapper mapper, Class<?> collectionClass, Class<?>... elementClasses) {
		return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
	}
}
