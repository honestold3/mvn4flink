package org.wq.thread;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by wq on 2016/10/29.
 */
public class TimerTaskDemo {

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                System.out.println("abc");
            }
        }, 3*1000 , 1000);
    }

}
