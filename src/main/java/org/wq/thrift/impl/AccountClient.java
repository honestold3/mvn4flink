package org.wq.thrift.impl;

import org.wq.thrift.generated.Account;
import org.wq.thrift.generated.InvalidOperation;
import org.wq.thrift.generated.Operation;
import org.wq.thrift.generated.Request;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by wq on 2016/10/7.
 */
public class AccountClient {
    public static void main(String[] args) throws TException {
        TTransport transport = new TSocket("localhost", 9999);
        transport.open();   //建立连接

        TProtocol protocol = new TBinaryProtocol(transport);
        Account.Client client = new Account.Client(protocol);

        //第一个请求， 登录 honest 帐号
        Request req = new Request("honest", "1234", Operation.LOGIN);
        request(client, req);

        //第二个请求， 注册 honest 帐号
        req.setOp(Operation.REGISTER);
        request(client, req);

        //第三个请求， 登录 honest 帐号
        req.setOp(Operation.LOGIN);
        request(client, req);

        //第四个请求， name 为空的请求
        req.setName("");
        request(client, req);

        transport.close();  //关闭连接

    }

    public static void request(Account.Client client, Request req) throws TException{
        try {
            String result = client.doAction(req);
            System.out.println(result);
        } catch (InvalidOperation e) {
            System.out.println(e.reason);
        }
    }
}
