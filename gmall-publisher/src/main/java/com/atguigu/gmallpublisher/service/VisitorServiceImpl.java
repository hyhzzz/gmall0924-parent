package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.Visitor;
import com.atguigu.gmallpublisher.mapper.VisitorMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:50
 */
@Service
public class VisitorServiceImpl implements VisitorService{

   @Autowired
   VisitorMapper visitorMapper;

   @Override
   public List<Visitor> visitor(int date) {
      return visitorMapper.visitor(date);
   }

}
