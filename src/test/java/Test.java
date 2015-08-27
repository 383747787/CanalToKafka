import org.apache.commons.lang.StringUtils;

/**
 * Created by lujia on 2015/8/25.
 */
public class Test {
    public static void main(String[] args) {
        String value = " ";
        if (StringUtils.isEmpty(value)) {
            System.out.println("YES");
        } else {
            System.out.println("NO");
        }
    }
}
