package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.annotation.NotSink;
import com.atguigu.gmall.realtime.bean.ProductStats;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-07 11:18
 */
public class AtguiguUtil {
    // 要么放常量 要么是静态方法

    public static <T> List<T> toList(Iterable<T> it) {
        List<T> result = new ArrayList<>();
        it.forEach(result::add);
        return result;
    }


    public static String toDateTime(Long ts, String... format) {

        String f = "yyyy-MM-dd HH:mm:ss";
        if (format.length != 0) {  // 没有传递时间格式, 使用一个默认格式
            f = format[0];
        }
        return new SimpleDateFormat(f).format(ts);
    }

    public static <T> String getFieldsString(Class<T> tClass) {

        String[] fieldNames = getFields(tClass);

        StringBuilder s = new StringBuilder();
        for (String fieldName : fieldNames) {
            s.append(fieldName).append(",");
        }
        s.deleteCharAt(s.length() - 1);

        return s.toString();
    }


    public static <T> String[] getFields(Class<T> tClass) {

        Field[] fields = tClass.getDeclaredFields();


        List<String> names = new ArrayList<>();

        for (Field field : fields) {
            // 对那些不需要sink的 field应该过滤掉
            //            if(field.getName().equals("orderIdSet") || ..)  // 这样写灵活度太低了
            NotSink noSink = field.getAnnotation(NotSink.class);
            if (noSink == null) { // 没有注解,表示这个属性要写出去
                names.add(field.getName());
            }
        }


        // 把list集合转成数组
       /* String[] result = new String[names.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = names.get(i);
        }*/
        return names.toArray(new String[0]);
    }

    public static Long toTs(String dateTime, String... format) throws ParseException {
        String f = "yyyy-MM-dd HH:mm:ss";
        if (format.length != 0) {  // 没有传递时间格式, 使用一个默认格式
            f = format[0];
        }
        return new SimpleDateFormat(f).parse(dateTime).getTime();
    }

    //利用ik分词器对 kw进行分词
    public static List<String> split(String kw) throws IOException {

        //字符串 -> ...  -> Reader
        // 内存流  StringReader
        IKSegmenter seg = new IKSegmenter(new StringReader(kw), true);

        Collection<String> words = new HashSet<>();
        Lexeme next = seg.next();

        while (next != null) {

            String word = next.getLexemeText();
            words.add(word);
            next = seg.next();
        }
        return new ArrayList<>(words);
    }
}
