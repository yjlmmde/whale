
import org.joni.Regex;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by yjl
 * Data: 2019/3/14
 * Time: 21:12
 */
public class FloatDouble {
    public static void main(String[] args) {
        double a = 1.1212;
        double c = 1.2121;
        float b = 1.1212f;
        BigDecimal subtract = new BigDecimal(a).subtract(new BigDecimal(b));
        BigDecimal sc = new BigDecimal(a).subtract(new BigDecimal(c));
        System.out.println(subtract);
        System.out.println(sc);
        System.out.println(b);
        System.exit(0);
        double abs = Math.abs(a - b);
        System.out.println(abs);
        System.out.println(a < b);
        String str = "abcdesf";
        String ss = "";
        String s = "";
        char[] ch = str.toCharArray();
        for (int i = 0; i < ch.length; i += 2) {
            ss += ch[i];
        }
        for (int i = 0; i < str.length(); i += 2) {
            s += str.substring(i, i + 1);
        }

        System.out.println(ss);
        System.out.println(s);
        StringBuilder sb = new StringBuilder();


    }
}
