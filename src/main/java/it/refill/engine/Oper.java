/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.refill.engine;

import java.util.LinkedList;

/**
 *
 * @author rcosco
 */
public class Oper {
    
    String type,sql;
    LinkedList<String> param;

    public Oper() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public LinkedList<String> getParam() {
        return param;
    }

    public void setParam(LinkedList<String> param) {
        this.param = param;
    }

}