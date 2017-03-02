package org.wq.mvn4flink.scalajava;

/**
 * Created by wq on 2016/10/11.
 */
public class JavaCatchScalaThrower {
    //Java中调用ScalaThrower(Scala类），然后捕获其抛出的异常
    public static void main(String[] args){
        ScalaThrower st=new ScalaThrower();
        try{
            st.exceptionThrower();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
