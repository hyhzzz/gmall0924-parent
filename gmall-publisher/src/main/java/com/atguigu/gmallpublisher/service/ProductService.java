package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.ProductStats;

import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-14 15:47
 */
public interface ProductService {
    BigDecimal gmv(int date);

    List<Map<String, Object>> gmvByTm(int date);

    List<ProductStats> getByC3(int date);

    List<ProductStats> getBySpu(int date);

}
