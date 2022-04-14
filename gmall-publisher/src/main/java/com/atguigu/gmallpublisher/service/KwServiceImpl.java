package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.Kw;
import com.atguigu.gmallpublisher.mapper.KwMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author coderhyh
 * @create 2022-04-14 18:56
 */
@Service
public class KwServiceImpl implements KwService {

    @Autowired
    KwMapper kwMapper;

    public List<Kw> kw(int date) {
        return kwMapper.kw(date);
    }

    ;
}
