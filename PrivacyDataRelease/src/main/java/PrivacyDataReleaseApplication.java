import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Scanner;

public class PrivacyDataReleaseApplication {
    public static void main(String[] args) throws IOException{


        int k=5,l=5,d=5;
        Scanner in = new Scanner(System.in);
        System.out.println("请输入参数：");

        System.out.println("k-anonymity:");
        k = in.nextInt();

        do {
            System.out.println("l-diverse(l<=14):");
            l = in.nextInt();
        }while (l>14 || l<0);

        System.out.println("d-扰动参数:");
        d = in.nextInt();

        System.out.println("输入参数为：k="+k+",l="+l+",d="+d);
        System.out.println("Processing!");
        in.close();



        String rowRecord = null;
        int count = 0;

        //文件读取
        FileReader reader = new FileReader("C:\\Users\\Administrator\\Desktop\\数据库安全实验\\Adult_Data_Set\\adult.data");
        File output = new File("C:\\Users\\Administrator\\Desktop\\数据库安全实验\\Adult_Data_Set\\adult.data.output");
        BufferedReader breader = new BufferedReader(reader);

//        while ((rowRecord=breader.readLine()) != null){
//            System.out.println(++count);
 //       }
        reader.close();
        breader.close();

        DataProcesser dp = new DataProcesser(7,7,7);
        dp.goPublish();
        dp.getFinalResultValueSet();
//        dp.getFinalResultValueSetNum();
//        dp.testFinalResulValueSetKlimit();
        dp.writeResult(output);
        System.out.println("Success!");

    }
}
