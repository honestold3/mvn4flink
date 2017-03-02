package org.wq.mvn4flink.scalajava;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wq on 2016/10/11.
 */
public class JavaGeneric {

    public static List<?> getList(){
        List<String> listStr=new ArrayList<String>();
        listStr.add("aaaa");
        listStr.add("bbbb");
        return listStr;
    }
}
