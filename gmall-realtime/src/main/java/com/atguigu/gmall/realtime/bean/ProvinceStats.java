package com.atguigu.gmall.realtime.bean;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author coderhyh
 * @create 2022-04-13 18:34
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProvinceStats {
    private Long province_id;
    private String province_name;
    private String area_code;
    private String iso_code;
    private String iso_3166_2;
    //    private Timestamp stt;
    //    private Timestamp edt;
    private String stt;
    private String edt;
    private BigDecimal order_amount;
    private Long order_count;
    private Long ts;
}
