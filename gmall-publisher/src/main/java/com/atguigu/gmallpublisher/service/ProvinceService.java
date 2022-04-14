package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.Province;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:33
 */
public interface ProvinceService {
    List<Province> statProvince(int date);
}
