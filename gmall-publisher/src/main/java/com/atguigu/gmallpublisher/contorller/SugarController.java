package com.atguigu.gmallpublisher.contorller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.bean.Kw;
import com.atguigu.gmallpublisher.bean.ProductStats;
import com.atguigu.gmallpublisher.bean.Province;
import com.atguigu.gmallpublisher.bean.Visitor;
import com.atguigu.gmallpublisher.service.KwService;
import com.atguigu.gmallpublisher.service.ProductService;
import com.atguigu.gmallpublisher.service.ProvinceService;
import com.atguigu.gmallpublisher.service.VisitorService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-14 15:51
 */
@RestController
public class SugarController {

    @Autowired
    ProductService productService;


    @RequestMapping("gmv")
    public String gmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }

        BigDecimal gmv = productService.gmv(date);
        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");
        result.put("date", gmv);

        return result.toString();
    }

    @RequestMapping("gmv/tm")
    public String gmvByTm(@RequestParam("date") int date) {


        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }

        List<Map<String, Object>> list = productService.gmvByTm(date);

        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();

        result.put("data", data);


        JSONArray categories = new JSONArray();

        for (Map<String, Object> map : list) {
            Object tm_name = map.get("tm_name");

            categories.add(tm_name);
        }
        data.put("categories", categories);

        JSONArray series = new JSONArray();
        result.put("series", series);

        JSONObject obj = new JSONObject();
        series.add(obj);

        obj.put("name", "商品品牌");
        JSONArray data1 = new JSONArray();
        obj.put("data", data1);

        for (Map<String, Object> map : list) {
            Object order_amount = map.get("order_amount");
            data1.add(order_amount);
        }

        return result.toString();
    }

    @RequestMapping("gmv/c3")
    public String gmvByC3(@RequestParam("date") int date) {


        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }


        List<ProductStats> list = productService.getByC3(date);

        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");

        JSONArray data = new JSONArray();
        for (ProductStats tm : list) {

            JSONObject obj = new JSONObject();
            obj.put("name", tm.getCategory3_name());
            obj.put("value", tm.getOrder_amount());

            data.add(obj);
        }

        result.put("data", data);

        return result.toString();
    }


    @RequestMapping("gmv/spu")
    public String gmvBySpu(@RequestParam("date") int date) {


        if (date == 0) {
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }


        List<ProductStats> list = productService.getBySpu(date);

        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();
        JSONArray columns = new JSONArray();
        JSONObject c1 = new JSONObject();
        c1.put("name", "spu名字");
        c1.put("id", "spu");
        columns.add(c1);


        JSONObject c2 = new JSONObject();
        c2.put("name", "销售额");
        c2.put("id", "order_amount");
        columns.add(c2);


        data.put("columns", columns);


        JSONArray rows = new JSONArray();
        result.put("rows", rows);
        for (ProductStats spu : list) {
            JSONObject row = new JSONObject();
            row.put("spu", spu.getSpu_name());
            row.put("order_amount", spu.getOrder_amount());
            rows.add(row);
        }


        result.put("data", data);


        return result.toString();
    }

    @Autowired
    ProvinceService provinceService;

    @RequestMapping("/province")
    public String province(@RequestParam("date") int date) {
        // 如果没有传递日期过来, date的默认值是0
        if (date == 0) { // 如果默认值0, 则给date赋值今天的日期
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }


        List<Province> list = provinceService.statProvince(date);


        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();

        JSONArray mapData = new JSONArray();

        for (Province province : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", province.getProvince_name());
            obj.put("value", province.getOrder_amount());

            JSONArray tooltipValues = new JSONArray();
            tooltipValues.add(province.getOrder_count());
            obj.put("tooltipValues", tooltipValues);

            mapData.add(obj);

        }

        data.put("mapData", mapData);

        data.put("valueName", "销售额");

        JSONArray tooltipNames = new JSONArray();
        tooltipNames.add("订单数");
        data.put("tooltipNames", tooltipNames);

        JSONArray tooltipUnits = new JSONArray();
        tooltipUnits.add("个");
        data.put("tooltipUnits", tooltipUnits);


        result.put("data", data);


        return result.toJSONString();

    }

    @Autowired
    VisitorService visitorService;

    @RequestMapping("/visitor")
    public String visitor(@RequestParam("date") int date) {
        // 如果没有传递日期过来, date的默认值是0
        if (date == 0) { // 如果默认值0, 则给date赋值今天的日期
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }


        List<Visitor> list = visitorService.visitor(date);
        Map<Integer, Visitor> hourToVisit = new HashMap<Integer, Visitor>();
        for (Visitor visitor : list) {
            hourToVisit.put(visitor.getHour(), visitor);
        }
        System.out.println(hourToVisit);


        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");

        JSONObject data = new JSONObject();

        JSONArray categories = new JSONArray();
        // 一定是24个小时 0-23
        for (int i = 0; i < 24; i++) {
            categories.add(i);
        }
        data.put("categories", categories);

        JSONArray series = new JSONArray();

        //第一个折线
        JSONObject pv = new JSONObject();
        pv.put("name", "pv");
        JSONArray pvData = new JSONArray();
        pv.put("data", pvData);
        series.add(pv);


        //第一个折线
        JSONObject uv = new JSONObject();
        uv.put("name", "uv");
        JSONArray uvData = new JSONArray();
        uv.put("data", uvData);
        series.add(uv);

        //第一个折线
        JSONObject uj = new JSONObject();
        uj.put("name", "uj");
        JSONArray ujData = new JSONArray();
        uj.put("data", ujData);
        series.add(uj);


        // pvData中, 一定是放入24个值
        for (int i = 0; i < 24; i++) {

            // i=0 hour是0点
            // 如果key不能存在, 则返回默认值
            Visitor visitor = hourToVisit.getOrDefault(i, new Visitor(i, 0L, 0L, 0L));
            System.out.println(visitor);
            pvData.add(visitor.getPv());
            uvData.add(visitor.getUv());
            ujData.add(visitor.getUj());
        }
        data.put("series", series);
        result.put("data", data);


        return result.toJSONString();
    }

    @Autowired
    KwService kwService;

    @RequestMapping("/kw")
    public String kw(@RequestParam("date") int date) {
        // 如果没有传递日期过来, date的默认值是0
        if (date == 0) { // 如果默认值0, 则给date赋值今天的日期
            date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
        }


        List<Kw> list = kwService.kw(date);


        JSONObject result = new JSONObject();

        result.put("status", 0);
        result.put("msg", "");

        JSONArray data = new JSONArray();

        for (Kw kw : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", kw.getKeyword());
            obj.put("value", kw.getScore());

            data.add(obj);
        }


        result.put("data", data);


        return result.toJSONString();
    }

}

