namespace java org.wq.thrift.generated

enum Operation{
  LOGIN = 1,
  REGISTER = 2
}

struct Request{
  1: string name,
  2: string password,
  3: Operation op
}

exception InvalidOperation{
  1: i32 code,
  2: string reason
}

service Account{
  string doAction(1: Request request) throws (1: InvalidOperation e);
}