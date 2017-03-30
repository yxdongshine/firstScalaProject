package com.yxd.bigdata.spark.kafka.FileStream;

import java.io.*;

/**
 * Created by 20160905 on 2017/3/28.
 */
public class FileIo {

    public  void writeFile(String strb,FileWriter fw){

        try {

                fw.write(strb.toString());
                fw.write("\r\n");//写入换行

        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.println(e.getMessage());
        }
    }



    public  void readFile(){
        FileReader fr = null;
        FileWriter fw=null;
        try {
            fr = new FileReader("english.txt");
            fw= new FileWriter("english2.txt");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            System.out.println(e.getMessage());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        BufferedReader bf=new BufferedReader(fr);
        BufferedWriter bw=new BufferedWriter(fw);
        String str=null;//取一行
        StringBuffer strb = null;//对string的插入操作需要用StringBuffer类型
        try {
            while((str=bf.readLine())!=null){  //一行一行的读取全部内容
                strb=new StringBuffer(str);
                if(str.lastIndexOf(" ")!=-1) { //判断是否存在 空格符
                    strb.insert(strb.indexOf(" "), "|");
                }
                else{
                    for(int i=0;i<strb.length();i++){
                        if(strb.charAt(i)>122)
                            strb.insert(i,"|");
                    }
                }
                fw.write(strb.toString());
                fw.write("\r\n");//写入换行
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.println(e.getMessage());
        }
    }
}
