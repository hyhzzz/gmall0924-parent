package com.atguigu.gmallpublisher.mapper;

import com.atguigu.gmallpublisher.bean.Visitor;

import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:49
 */
public interface VisitorMapper {

   @Select("SELECT\n" +
           "    toHour(stt) hour,\n" +
           "    sum(pv_ct) AS pv,\n" +
           "    sum(uv_ct) AS uv,\n" +
           "    sum(uj_ct) AS uj\n" +
           "FROM visitor_stats_2022\n" +
           "WHERE toYYYYMMDD(stt) = #{date}\n" +
           "GROUP BY toHour(stt)")
   List<Visitor> visitor(int date);
}
