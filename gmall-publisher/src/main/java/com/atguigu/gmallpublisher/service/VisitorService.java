package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.Visitor;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:50
 */
public interface VisitorService {
   List<Visitor> visitor(int date);
}
