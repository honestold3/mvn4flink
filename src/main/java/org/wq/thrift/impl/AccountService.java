package org.wq.thrift.impl;

import org.wq.thrift.generated.Account;
import org.wq.thrift.generated.InvalidOperation;
import org.wq.thrift.generated.Operation;
import org.wq.thrift.generated.Request;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wq on 2016/10/7.
 */
public class AccountService implements Account.Iface {
    private static Map<String, String> accounts = new HashMap<>();

    @Override
    public String doAction(Request request) throws InvalidOperation {
        String name = request.getName();
        String pass = request.getPassword();
        Operation op = request.getOp();

        System.out.println(String.format("Get request[name:%s, pass:%s, op:%d]", name, pass, op.getValue()));

        if (name == null || name.length() == 0){
            throw new InvalidOperation(100, "param name should not be empty");
        }

        if (op == Operation.LOGIN) {
            String password = accounts.get(name);
            if (password != null && password.equals(pass)) {
                return "Login success!! Hello " + name;
            } else {
                return "Login failed!! please check your username and password";
            }
        } else if (op == Operation.REGISTER) {
            if (accounts.containsKey(name)) {
                return String.format("The username '%s' has been registered, please change one.", name);
            } else {
                accounts.put(name, pass);
                return "Register success!! Hello " + name;
            }
        } else {
            throw new InvalidOperation(101, "unknown operation: " + op.getValue());
        }
    }
}
