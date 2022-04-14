package com.atguigu.gmallpublisher.mapper;

import com.atguigu.gmallpublisher.bean.Kw;

import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:55
 */
public interface KwMapper {

   @Select("SELECT\n" +
           "    keyword,\n" +
           "    sum(ct * multiIf(source = 'search', 2, source = 'click', 4, source = 'cart', 6, 8)) AS score\n" +
           "FROM keyword_stats_2022\n" +
           "WHERE toYYYYMMDD(stt) = #{date}\n" +
           "GROUP BY keyword\n")
   List<Kw> kw(int date);
}
