/**
 * Created by wq on 16/9/21.
 */
class Ff {

    int i = 10;

    int j = i;

}

class Ss extends Ff {

    int i = 20;

}

class Kankan {
    public static void main(String[] args) {
        System.out.println(new Ss().j);
    }
}
