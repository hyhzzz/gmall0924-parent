package com.atguigu.gmall.realtime.function;

import com.atguigu.gmall.realtime.util.AtguiguUtil;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-13 18:59
 */
@FunctionHint(output = @DataTypeHint("row<word string>"))
public class IkAnalyzer extends TableFunction<Row> {
    public void eval(String kw) throws IOException {
        // 把kw 进行切词   小米手机 -> 小米 手机
        List<String> words = AtguiguUtil.split(kw);

        for (String word : words) {
            collect(Row.of(word));
        }
    }
}
