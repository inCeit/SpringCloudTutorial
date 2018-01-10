import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Scanner;

public class test {
    public static void main(String[] args) throws IOException{

/*
        DataProcesser dp = new DataProcesser();
        dp.fillData().fin1();
        dp.getTableValueSetNum();
        System.out.println("Success!");
    }
    */
        int k=5,l=5,d=0;
        String fileNmae;
        Scanner in = new Scanner(System.in);
        System.out.println("请输入参数：");

        do {
            System.out.println("k-anonymity(k>0):");
            k = in.nextInt();
        }while (k < 0);


        do {
            System.out.println("l-diverse(0<l<=14):");
            l = in.nextInt();
        }while (l>14 || l<0);

        do {
            System.out.println("d-扰动参数(d根据数据实际情况指定，d>=0):");
            d = in.nextInt();
        }while (d < 0);

        System.out.println("输出文件名（仅指定文件名，在程序目录输出）：");
        fileNmae = in.next();



        System.out.println("输入参数为：k="+k+",l="+l+",d="+d);
        System.out.println("Start Process!");
        in.close();

        File directory = new File(".");
        String path = directory.getCanonicalPath();
        path += "\\Adult_Data_Set\\";
        //文件读取
        FileReader reader = new FileReader(path+"\\adult.data");
        File output = new File(path+fileNmae);
        if (!output.exists()){
            output.createNewFile();
        }

        BufferedReader breader = new BufferedReader(reader);


        DataProcesser dp = new DataProcesser(k,l,d,breader);
        // 1:从数据库读取，0:从adult.data文件读取
        dp.goPublish(1);
//        dp.getFinalResultValueSet();
//        dp.getFinalResultValueSetNum();
//        dp.testFinalResulValueSetKlimit();
//        dp.fillDataFromFile(breader);
        dp.writeResult(output);
        reader.close();
        breader.close();

        System.out.println("Process Finish!");
    }
}
