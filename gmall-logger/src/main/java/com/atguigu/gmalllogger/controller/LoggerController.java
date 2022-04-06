package com.atguigu.gmalllogger.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

/**
 * @author coderhyh
 * @create 2022-04-06 21:45
 */
@RestController
@Slf4j
public class LoggerController {

    @GetMapping("/applog")  // 提交路径
    public String logger(@RequestParam("param") String jsonStr) {


        //        System.out.println(jsonStr);

        //把数据写入到磁盘  给离线准备
        saveToDisk(jsonStr);

        //把日志数据写入到kafka中 形成ods层
        sendToKafka(jsonStr);

        return "ok";
    }

    @Autowired
    private KafkaTemplate<String, String> kafka;


    private void sendToKafka(String jsonStr) {
        kafka.send("ods_log", jsonStr);

    }

    private void saveToDisk(String jsonStr) {
        log.info(jsonStr);
    }
}

