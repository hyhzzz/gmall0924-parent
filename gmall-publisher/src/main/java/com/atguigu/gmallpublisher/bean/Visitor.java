package com.atguigu.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-14 18:49
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Visitor {
   private Integer hour;
   private Long pv;
   private Long uv;
   private Long uj;
}
