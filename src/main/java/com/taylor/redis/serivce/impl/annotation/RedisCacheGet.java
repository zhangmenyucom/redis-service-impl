package com.taylor.redis.serivce.impl.annotation;

import java.lang.annotation.*;


@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface RedisCacheGet {
    //枚举类型
    public enum DataType {
        CLASS, JSON
    }

    /**
     * 保存数据库
     *
     * @return
     */
    public int DBIndex() default 0;

    /**
     * key值
     *
     * @return
     */
    public String key();

    /**
     * 缓存过期时间默认为不过期，过期时间手动去设定，单位为 S
     * 0:不限制保存时长
     *
     * @return
     */
    public int expire() default 0;

    /**
     * force是否强制刷新数据
     *
     * @return
     */
    public boolean force() default false;

    /**
     * 数据类型
     *
     * @return
     */
    public DataType dataType() default DataType.JSON;

}

/*
    <aop:config>
        <aop:aspect ref="redisCacheAspect">
           <!--配置扫描的包-->
            <aop:pointcut expression="execution(* foo.bar.*.*(..))" id="redisCachePoint"/>
            <aop:around method="around(org.aspectj.lang.ProceedingJoinPoint)" pointcut-ref="redisCachePoint" />
        </aop:aspect>
    </aop:config>
*/
