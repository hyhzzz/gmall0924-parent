package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.Province;
import com.atguigu.gmallpublisher.mapper.ProvinceMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:34
 */
@Service
public class ProvinceServiceImp implements ProvinceService {


    @Autowired
    ProvinceMapper provinceMapper;
    @Override
    public List<Province> statProvince(int date) {
        return provinceMapper.statProvince(date);
    }
}
