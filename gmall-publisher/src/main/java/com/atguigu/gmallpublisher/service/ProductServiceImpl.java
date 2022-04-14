package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.ProductStats;
import com.atguigu.gmallpublisher.mapper.ProductMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-14 15:48
 */
@Service
public class ProductServiceImpl implements ProductService {

    @Autowired
    ProductMapper productMapper;

    @Override
    public BigDecimal gmv(int date) {
        return productMapper.gmv(date);
    }

    @Override
    public List<Map<String, Object>> gmvByTm(int date) {
        return productMapper.gmvByTm(date);
    }

    @Override
    public List<ProductStats> getByC3(int date) {
        return productMapper.getByC3(date);
    }

    @Override
    public List<ProductStats> getBySpu(int date) {
        return productMapper.getBySpu(date);
    }

}
