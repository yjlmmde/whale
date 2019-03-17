import scala.math.Ordering;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by yjl
 * Data: 2019/2/27
 * Time: 23:50
 */
public class FileRead {
    /*public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("f:/dmp/app_dict.txt"));
        String str = br.readLine();
        System.out.println(str);
        String[] split = str.split("\\s+");
        for (String s : split) {
            System.out.println(s);
        }
        br.close();

    }*/
    public static void main(String[] args){
        //比较学生对象
        Student stu1 = new Student("joffy",21);
        Student stu2 = new Student("joffy",21);
        Student stu3 = new Student("joffy",22);
        System.out.println("stu1.equals(stu2):"+stu1.equals(stu2));
        System.out.println("stu1.equals(stu3):"+stu1.equals(stu3));
        String j1 = new String("java");
        String j2 = new String("java");
        String jj1 = "java";
        String jj2 = "java";
        System.out.println(j1.equals(j2));
        System.out.println("j1==j2: "+(j1==j2));
        System.out.println("jj1==jj2: "+(jj1==jj2));

    }
}

class Student
{
    String name;
    int age;
    public Student(String name,int age){
        this.name=name;
        this.age=age;
    }
    //比较方法，只有姓名和年龄相等才相等
    public boolean equals(Student obj){
        Student student = obj;
        return this.name.equals(student.name)&&this.age==student.age;
    }
}