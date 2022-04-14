package com.atguigu.gmall.realtime.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author coderhyh
 * @create 2022-04-12 9:34
 */
// 注解的使用范围: 这个注解用在哪
@Target(ElementType.FIELD)
// 注解的生命周期
@Retention(RetentionPolicy.RUNTIME)
public @interface NotSink {
}

