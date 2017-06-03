package org.wq.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;


/**
 * Created by wq on 15/2/1.
 */
public class NewThreadDemo {
    public static void main(String[] args) throws Exception {
        Thread t1 = new Thread(){

            @Override
            public void run(){
                System.out.println("new t1");
            }
        };
        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("new t2");
            }
        });
        t2.start();

        FutureTask<String> ft = new FutureTask<String>(new Callable<String>() {
            @Override
            public String call() throws Exception {
                System.out.println("new t3");
                Thread.sleep(3000);
                return "kankan";
            }
        });
        Thread t3 = new Thread(ft);
        t3.start();
        //System.out.println("hehe:"+ft.get());


        FutureTask<String> ft1 = null;
        Thread t4 = new Thread(ft1 = new FutureTask<String>(new Callable<String>() {
            @Override
            public String call() throws Exception {
                System.out.println("new t4");
                return "kankan1";
            }
        }));
        t4.start();
        System.out.println(ft1.get());

        System.out.println(Thread.currentThread().getName());

        //WatchService watchService = FileSystems.getDefault().newWatchService();
    }
}
