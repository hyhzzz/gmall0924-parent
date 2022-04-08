package com.atguigu.gmall.realtime.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-07 11:18
 */
public class AtguiguUtil {
    public static <T> List<T> toList(Iterable<T> it) {
        ArrayList<T> result = new ArrayList<>();

        it.forEach(result::add);
        return result;
    }
}
