package org.wq.jol;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.vm.VM;

import java.util.ArrayList;
import java.util.LinkedList;

import static java.lang.System.out;


/**
 * Created by wq on 2017/1/13.
 */
public class JOLDemo {

    public static void main(String[] args) throws Exception {

        System.out.println(VM.current().details());
        System.out.println("--------------------------");

        System.out.println("Graph:"+GraphLayout.parseInstance("asdf").toPrintable());
        System.out.println("total:"+GraphLayout.parseInstance("asdf").totalSize());
        System.out.println("");
        System.out.println("Detail Class:"+ClassLayout.parseClass("asdf".getClass()).toPrintable());
        System.out.println("Detail Instance:"+ClassLayout.parseInstance("asdf").toPrintable());

        System.out.println("--------------------------");
    }

}
