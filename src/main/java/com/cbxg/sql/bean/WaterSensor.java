package com.cbxg.sql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author:cbxg
 * @date:2021/4/5
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    private  String id;
    private  Long ts;
    private  int vc;

//    public String getId() {
//        return id;
//    }
//
//    public void setId(String id) {
//        this.id = id;
//    }
//
//    public Long getTs() {
//        return ts;
//    }
//
//    public void setTs(Long ts) {
//        this.ts = ts;
//    }
//
//    public int getVc() {
//        return vc;
//    }
//
//    public void setVc(int vc) {
//        this.vc = vc;
//    }
//
//    public WaterSensor(String id, Long ts, int vc) {
//        this.id = id;
//        this.ts = ts;
//        this.vc = vc;
//    }
}
