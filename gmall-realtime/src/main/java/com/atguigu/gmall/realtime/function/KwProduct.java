package com.atguigu.gmall.realtime.function;

import com.atguigu.gmall.realtime.bean.SourceCt;

import org.apache.flink.table.functions.TableFunction;

/**
 * @author coderhyh
 * @create 2022-04-14 0:38
 */
public class KwProduct extends TableFunction<SourceCt> {
    public void eval(Long click_ct, Long cart_ct, Long order_ct) {
        if (click_ct > 0) {
            collect(new SourceCt("click", click_ct));
        }

        if (cart_ct > 0) {
            collect(new SourceCt("cart", cart_ct));
        }

        if (order_ct > 0) {
            collect(new SourceCt("order", order_ct));
        }


    }
}
