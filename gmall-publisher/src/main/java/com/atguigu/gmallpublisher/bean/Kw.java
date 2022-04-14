package com.atguigu.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-14 18:55
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Kw {
   private String keyword;
   private Long score;
}
