package org.wq.jit;

/**
 * Created by wq on 16/9/1.
 */
public class Test {

    //-Xint
    //-Xcomp

    public static double calcPi(){
        double re=0;
        for(int i=1;i<10000;i++){
            re+=((i&1)==0?-1:1)*1.0/(2*i-1);
            //re +=  4*Math.pow(-1,i)/(2*i+1);
        }
        return re*4;
        //return re;
    }
    public static void main(String[] args) {
        long b= System.currentTimeMillis();
        for(int i=0;i<10000;i++)
            calcPi();
        long e= System.currentTimeMillis();
        System.out.println("spend:"+(e-b)+"ms");
    }

}
