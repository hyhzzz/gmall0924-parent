package com.atguigu.gmallpublisher.mapper;

import com.atguigu.gmallpublisher.bean.ProductStats;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-14 15:37
 */
public interface ProductMapper {
    // toYYYYMMDD(stt)是把日志转成数字的函数(ClickHouse) 比如:2021-03-01 -> 20210301

    @Select(
            "select " +
                    "   sum(order_amount) order_amount " +
                    "from product_stats_2022 " +
                    "where toYYYYMMDD(stt)=#{date}")
    BigDecimal gmv(@Param("date") int date);


    @Select(
            "select " +
                    "   tm_id," +
                    "   tm_name, " +
                    "   sum(order_amount) order_amount " +
                    "from product_stats_2022 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by tm_id,tm_name ")
    List<Map<String, Object>> gmvByTm(@Param("date") int date);


    @Select(
            "select " +
                    "   category3_id," +
                    "   category3_name," +
                    "   sum(order_amount) order_amount " +
                    "   from product_stats_2022 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by category3_id,category3_name ")
    List<ProductStats> getByC3(@Param("date") int date);


    @Select(
            "select " +
                    "   spu_id," +
                    "   spu_name," +
                    "   sum(order_amount) order_amount," +
                    "   sum(order_ct) order_ct " +
                    "from product_stats_2022 " +
                    "where toYYYYMMDD(stt)=#{date} " +
                    "group by spu_id,spu_name ")
    List<ProductStats> getBySpu(@Param("date") int date);



}
