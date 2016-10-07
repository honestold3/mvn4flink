package org.wq.thrift.impl;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.wq.thrift.generated.Account;

/**
 * Created by wq on 2016/10/7.
 */
public class AccountServer {

    public static void main(String[] args) throws Exception {
        TServerSocket socket = new TServerSocket(9999);
        Account.Processor processor = new Account.Processor<>(new AccountService());
        TServer server = new TSimpleServer(new TServer.Args(socket).processor(processor));
        System.out.println("Starting the Account server...");
        server.serve();
    }

}
