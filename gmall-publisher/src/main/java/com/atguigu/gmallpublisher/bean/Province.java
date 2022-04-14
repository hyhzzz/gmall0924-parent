package com.atguigu.gmallpublisher.bean;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-14 18:29
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Province {
   private String province_name;
   private BigDecimal order_amount;
   private Long order_count;
}
