package ssgVideo;

public class ETLUtils {

    public static String etlData(String srcData){
        StringBuffer resultData = new StringBuffer();


        String[] datas = srcData.split("\t");
        //2. 判断长度是否小于9
        if(datas.length <9){
            return null ;
        }
        //3. 将数据中的视频类别的空格去掉
        datas[3]=datas[3].replaceAll(" ","");
        //4. 将数据中的关联视频id通过&拼接
        for (int i = 0; i < datas.length; i++) {
            if(i < 9){
                //4.1 没有关联视频的情况
                if(i == datas.length-1){
                    resultData.append(datas[i]);
                }else{
                    resultData.append(datas[i]).append("\t");
                }
            }else{
                //4.2 有关联视频的情况
                if(i == datas.length-1){
                    resultData.append(datas[i]);
                }else{
                    resultData.append(datas[i]).append("&");
                }
            }
        }
        return resultData.toString();


    }
}
