package com.atguigu.gmalllogger.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author coderhyh
 * @create 2022-04-06 21:45
 */
@RestController
public class LoggerController {

    @GetMapping("/applog")  // 提交路径
    public String doLog(@RequestParam("param") String logJson) {
        System.out.println(logJson);
        return "ok";
    }

}

