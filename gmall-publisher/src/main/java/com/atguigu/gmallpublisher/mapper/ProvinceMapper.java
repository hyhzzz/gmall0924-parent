package com.atguigu.gmallpublisher.mapper;

import com.atguigu.gmallpublisher.bean.Province;

import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:31
 */
public interface ProvinceMapper {

    @Select("SELECT\n" +
            "    province_name,\n" +
            "    sum(order_amount) AS order_amount,\n" +
            "    sum(order_count) AS order_count\n" +
            "FROM province_stats_2022\n" +
            "WHERE toYYYYMMDD(stt) = #{date}\n" +
            "GROUP BY province_name")
    List<Province> statProvince(int date);
}
