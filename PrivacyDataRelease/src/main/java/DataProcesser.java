

import jdk.nashorn.internal.objects.annotations.Getter;
import jdk.nashorn.internal.objects.annotations.Setter;

import sun.awt.image.ImageWatched;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

import java.util.*;

public class DataProcesser {

    private LinkedList<RowNode> rowList = new LinkedList<RowNode>();
    private int kValue=7;
    private int lValue=7;
    private int dValue=5;
    private TreeNode root= null;
//    private int count= 0;

    //构造函数，初始化k,l,d值
    public DataProcesser(int k,int l,int d){
        this.kValue = k;
        this.lValue = l;
        this.dValue = d;
    }

    //填充数据，从MySQL获取数据，构造RowNode对象
    public DataProcesser fillData(){
        try{
            String url="jdbc:mysql://localhost:3306/dbsecurity";    //JDBC的URL
            Connection conn;
            conn = DriverManager.getConnection(url,"root","Abc123!");
            Statement stmt = conn.createStatement(); //创建Statement对象
            System.out.println("成功连接到数据库！");

            String sql = "select * from adult";    //要执行的SQL

            ResultSet rs = stmt.executeQuery(sql);//创建数据对象
            while (rs.next()){
                RowNode row = new RowNode();
                row.setAge(rs.getInt(1));
                row.setWorkclass(rs.getString(2));
                row.setFnlwgt(rs.getInt(3));
                row.setEducation(rs.getString(4));
                row.setEducation_num(rs.getInt(5));
                row.setMarital_status(rs.getString(6));
                row.setOccupation(rs.getString(7));
                row.setRelationship(rs.getString(8));
                row.setRace(rs.getString(9));
                row.setSex(rs.getString(10));
                row.setCapital_gain(rs.getInt(11));
                row.setCapital_loss(rs.getInt(12));
                row.setHours_per_week(rs.getInt(13));
                row.setNative_country(rs.getString(14));
                row.setIncome(rs.getBoolean(15));
                rowList.add(row);
//                System.out.println(row.toString());
//                count++;
            }
            Collections.sort(rowList, new RowNodeComparator());
            System.out.println(rowList.size());
            int i=0;
//            for (Iterator iterator = rowList.listIterator();iterator.hasNext()&& i<=200;i++){
//                RowNode rowNode = (RowNode)iterator.next();
//                System.out.println(rowNode.toString());
 //           }
            rs.close();
            stmt.close();
            conn.close();
//            System.out.println(count);
        }catch(Exception e){
            e.printStackTrace();
        }
        return this;
    }

    //用当前未用最小QID划分ValueSet,其中最小未用是ValueSet的qids属性
    public LinkedList<ValueSet> partition(ValueSet vs){
        vs.qids--;
        LinkedList<ValueSet> vsList = new LinkedList<ValueSet>();
        switch (vs.qids){
            case 7:{
                String flag = vs.list.getFirst().race;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.removeFirst();
                    if (tmpRowNode.race.compareTo(flag) == 0){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.race;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
            case 6:{
                String flag = vs.list.getFirst().workclass;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.remove();
                    if (tmpRowNode.workclass.compareTo(flag) == 0){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.workclass;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
            case 5:{
                String flag = vs.list.getFirst().marital_status;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.remove();
                    if (tmpRowNode.marital_status.compareTo(flag) == 0){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.marital_status;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
            case 4:{
                int flag = vs.list.getFirst().education_num;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.remove();
                    if (tmpRowNode.education_num == flag){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.education_num;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
            case 3:{
                String flag = vs.list.getFirst().native_country;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.remove();
                    if (tmpRowNode.native_country.compareTo(flag) == 0){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.native_country;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
            case 2:{
                int flag = vs.list.getFirst().age;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.remove();
                    if (tmpRowNode.age == flag){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.age;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
            case 1:{
                int flag = vs.list.getFirst().hours_per_week;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.remove();
                    if (tmpRowNode.hours_per_week == flag){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.hours_per_week;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
            case 0:{
                int flag = vs.list.getFirst().capital_gain;
                ValueSet tmpVs = new ValueSet();
                while (vs.list.size() != 0){
                    RowNode tmpRowNode = vs.list.remove();
                    if (tmpRowNode.capital_gain == flag){
                        tmpVs.list.add(tmpRowNode);
                    }else {
                        tmpVs.qids=vs.qids;
                        vsList.add(tmpVs);
                        flag = tmpRowNode.capital_gain;
                        tmpVs = new ValueSet();
                        tmpVs.list.add(tmpRowNode);
                    }
                }
                tmpVs.qids=vs.qids;
                vsList.add(tmpVs);
                break;
            }
         }
         return vsList;
    }

    //第一次划分：创建划分树
    public DataProcesser fin1(){

        ValueSet T = new ValueSet();
        T.list = rowList;
        createTree(T,null,kValue,lValue,5,5);
//        testReadTreeData(root);
        return this;
    }

    /*  遍历第一次划分树，确定构造是否正确
    public void testRead(){
        testReadTreeData(root);
    }
    */

    /*  遍历第一次划分树，确定构造是否正确
    int ccc=0;
    public void testReadTreeData(TreeNode treeNode){

        if (treeNode.child.size()==0){
            for (Iterator iterator = treeNode.vs.list.iterator();iterator.hasNext();){
                ccc++;
                System.out.println(iterator.next().toString());
            }
        }else {
            for (Iterator iterator = treeNode.child.iterator();iterator.hasNext();){
                testReadTreeData((TreeNode)iterator.next());
            }
        }
        System.out.println(ccc);
//        System.out.println(mmm);
    }
    */

    int mmm=0;  //测试变量
    //创建第一次划分的划分树
    public TreeNode createTree(ValueSet g,TreeNode parent,int k,int l,int n,int p){

        TreeNode currentNode = new TreeNode(g);
        currentNode.setParent(parent);
        if (currentNode.getParent() ==  null){
            root = currentNode;
        }

        //此处应该添加随机性！
        LinkedList<ValueSet> valueSetLinkedList = partition(g);

        boolean continuePartion = true;

        for (Iterator iterator = valueSetLinkedList.iterator();iterator.hasNext();){
            ValueSet tmpVs = (ValueSet)iterator.next();
            if ((tmpVs.list.size()<2*k) || (getValueSetSentiveValueNum(tmpVs)<=l) || (tmpVs.qids<=1)){
                continuePartion = false;
            }
        }
        if (!continuePartion){
            for (Iterator iterator = valueSetLinkedList.iterator();iterator.hasNext();){
                ValueSet tmpVs = (ValueSet)iterator.next();
                g.list.addAll(tmpVs.list);
            }
            g.qids++;
        }


        if (continuePartion){
            for (Iterator iterator = valueSetLinkedList.iterator();iterator.hasNext();){
                ValueSet tmpVs = (ValueSet)iterator.next();
                currentNode.addChild(createTree(tmpVs,currentNode,k,l,n,p));
            }
        }else {
            ValueSet tmpVs = g;
            mmm+=g.list.size();
            TreeNode tmpTreeNode = new TreeNode(tmpVs);
            currentNode = tmpTreeNode;
            currentNode.setParent(parent);
        }
     return currentNode;
    }

    //测试函数，获取一个ValueSet的SA值的个数
    public int getValueSetSentiveValueNum(ValueSet vs){

        LinkedList<RowNode> list = vs.list;
        HashSet<String> valueSet = new HashSet<String>();

        for (Iterator iterator = list.listIterator();iterator.hasNext();){
            RowNode rowNode = (RowNode) iterator.next();
            if (valueSet.contains(rowNode.occupation)){
                continue;
            }else {
                valueSet.add(rowNode.occupation);
            }
        }
        return valueSet.size();
    }

    //获取一个ValueSet的SA值，用于生成最后分组的SA值集
    public String getValueSetSensitiveValueSet(ValueSet vs){
        HashSet<String> vsSet = new HashSet<String>();
        String result = "{";
        String split = "|";
        for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
            RowNode rowNode = (RowNode)iterator.next();
            vsSet.add(rowNode.occupation);
        }
        for (Iterator iterator = vsSet.iterator();iterator.hasNext();){
            String tmp = (String)iterator.next();
            result = result+tmp+split;
        }
        return result + "}";
    }

    int toburbation=0;
    //算法R(G,l)实现        =====没问题，没有改变vs.list的size
    public void toTurbation(ValueSet vs,int l){
        LinkedList<RowNode> list = vs.list;
        LinkedList<String> sensitiveValueList = new LinkedList<String>();
        for (Iterator iterator = list.listIterator();iterator.hasNext();){
            RowNode rowNode = (RowNode) iterator.next();
            sensitiveValueList.add(rowNode.occupation);
        }
        for (Iterator iterator = list.listIterator();iterator.hasNext();){
            toburbation++;
            int limit = sensitiveValueList.size();
            RowNode currentNode = (RowNode) iterator.next();
            while (currentNode.sa.size()< l-1){
                Random random = new Random();
                String tmpSA = (String)sensitiveValueList.get(random.nextInt(limit));
                if ((tmpSA.compareTo(currentNode.occupation)!=0) && (!currentNode.sa.contains(tmpSA))){
                    currentNode.sa.add(tmpSA);
                }
            }
        }
    }

    //算法G（t,k,d）实现      =======没问题，没有改变vs.list的size
    public ValueSet toGenerate(ValueSet vs,int k,int d){
        HashSet<String> sj = new HashSet<String>();
        HashSet<String> saj = new HashSet<String>();
        HashSet<String> sgj = new HashSet<String>();
        String generalizeResult="{";
        for (Iterator iterator = vs.list.iterator();iterator.hasNext();){
            RowNode rowNode = (RowNode)iterator.next();
            sj.add(rowNode.occupation);
            for (Iterator iterator1 = rowNode.sa.iterator();iterator1.hasNext();){
                saj.add((String) iterator1.next());
            }
        }
        sgj.addAll(sj);

        LinkedList<String> tmpList = new LinkedList<String>();
        tmpList.addAll(sj);
        tmpList.addAll(saj);
        LinkedList<String> sajList = new LinkedList<String>();
        sajList.addAll(saj);

        if (tmpList.size()> (vs.list.size()+d)){

            int i = tmpList.size()-vs.list.size()-d;
            Random random = new Random();

            for (int j=0;j<i;){
                int m = random.nextInt(sajList.size());
                if (sj.contains(sajList.get(m))){
                    continue;
                }else {
                    saj.remove(sajList.get(m));
                    j++;
                }
            }
            sgj.addAll(saj);
        }
        sgj.addAll(saj);

        String race = getSet(vs,1);
        String workclass = getSet(vs,2);
        String maritial_status = getSet(vs,3);
        String education_num = getRange(vs,4);
        String native_country = getSet(vs,5);
        String age = getRange(vs,6);
        String hours_per_week = getRange(vs,7);
        String capital_gain = getRange(vs,8);

        for (Iterator iterator = vs.list.iterator();iterator.hasNext();){
            RowNode rowNode = (RowNode)iterator.next();
            rowNode.raceSet = race;
            rowNode.workclassSet = workclass;
            rowNode.maritial_statusSet = maritial_status;
            rowNode.education_numRange = education_num;
            rowNode.native_countrySet = native_country;
            rowNode.ageRange = age;
            rowNode.hours_per_weekRange = hours_per_week;
            rowNode.capital_gainRange = capital_gain;
        }

        for (Iterator iterator = sgj.iterator();iterator.hasNext();){
            String tmp = (String)iterator.next();
            generalizeResult = generalizeResult + "|"+tmp;
        }
        vs.generalizeResult = generalizeResult+"}";
        return vs;
    }

    LinkedList<ValueSet> tableValueSet = new LinkedList<ValueSet>();

    //测试函数，测试fin1划分后得到的tableValueSet的记录数是否正确
    public void getTableValueSetNum(){
        prePublish(root);
        int i = 0;
        for (Iterator iterator = tableValueSet.iterator();iterator.hasNext();){
            i += ((ValueSet)iterator.next()).list.size();
        }
        System.out.println("测试日志：tableValueSet的记录条数为："+i);
    }
    public LinkedList<ValueSet> getTableValueSet() {
        return tableValueSet;
    }

    //此函数大概率没问题
    public void prePublish(TreeNode treeNode){
        if (treeNode.child.size()==0){
            toTurbation(treeNode.vs,lValue);
            tableValueSet.add(treeNode.vs);
        }else {
            for (Iterator iterator = treeNode.child.iterator();iterator.hasNext();){
                prePublish((TreeNode)iterator.next());
            }
        }
//        System.out.println(toburbation);
    }

    //重点检查此函数
    public LinkedList<ValueSet> toMoveOrMerge(LinkedList<ValueSet> vsList){
        LinkedList<ValueSet> resultValueSetList = new LinkedList<ValueSet>();
        //Debug
        int testDeleteZero = getVsListNum(vsList);
        for (Iterator iterator = vsList.iterator();iterator.hasNext();){
            ValueSet tmpVs = (ValueSet)iterator.next();
            if (tmpVs.list.size() == 0){
                iterator.remove();
            }
        }
        if (getVsListNum(vsList) != testDeleteZero){
            System.out.println("测试日志：MoveOrMerge函数去0前后数据不一致");
        }

        for (Iterator iterator = vsList.iterator();iterator.hasNext();){
            ValueSet tmpVs = (ValueSet)iterator.next();
            if (tmpVs.list.size() >= kValue ){
                //判断前面是否有小于K的值集
                if (resultValueSetList.size() > 0){
                    ValueSet lastValueSet = resultValueSetList.getLast();
                    if (lastValueSet.list.size()+tmpVs.list.size() >= 2*kValue){
                        int j = lastValueSet.list.size();
                        for (int i=0;i<(kValue-j);i++){
                            lastValueSet.list.add(tmpVs.list.remove());
                        }
                        resultValueSetList.addLast(tmpVs);
                    }else {
                        lastValueSet.list.addAll(tmpVs.list);
                    }
                }else {
                    resultValueSetList.addLast(tmpVs);
                }
            }else {
                if (resultValueSetList.size() > 0){
                    ValueSet neighbor = resultValueSetList.getLast();
                    if (neighbor.list.size()+tmpVs.list.size() >= 2*kValue){
                        int j = tmpVs.list.size();
                        for (int i = 0;i<(kValue-j);i++){
                            tmpVs.list.add(neighbor.list.remove());
                        }
                        resultValueSetList.add(tmpVs);
                    }else {
                        neighbor.list.addAll(tmpVs.list);
                    }
                }else {
                    if (iterator.hasNext()){
                        ValueSet neighbor = (ValueSet) iterator.next();
                        if (neighbor.list.size()+tmpVs.list.size() >= 2*kValue){
                            int j = tmpVs.list.size();
                            for (int i = 0;i < kValue-j;i++){
                                tmpVs.list.add(neighbor.list.remove());
                            }
                            resultValueSetList.add(tmpVs);
                            resultValueSetList.add(neighbor);
                        }else {
                            tmpVs.list.addAll(neighbor.list);
                            resultValueSetList.add(tmpVs);
                        }
                    }else {
                        resultValueSetList.add(tmpVs);
                    }
                }

            }
        }
        if (getVsListNum(resultValueSetList) != testDeleteZero){

            System.out.println("测试日志：MoveOrMerge函数去MoveMerge前后数据不一致");
            try {
                Thread.sleep(5000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return resultValueSetList;
    }

    //测试函数，获得LinkedList<ValueSet>中的记录数
    public int getVsListNum(LinkedList<ValueSet> vsList){
        int i = 0;
        for (Iterator iterator = vsList.iterator();iterator.hasNext();){
            i += ((ValueSet)iterator.next()).list.size();
        }
        return i;
    }

    //改进后使用全表ValueSet的Publish方法！ 该函数为算法4的部分实现
    public LinkedList<ValueSet> finalPublish(ValueSet vs){
        LinkedList<ValueSet> tmpVsList = new LinkedList<ValueSet>();
        if (vs.qids < 0){
            tmpVsList.add(toGenerate(vs,kValue,dValue));
        }else {
            LinkedList<ValueSet> valueSetLinkedList = new LinkedList<ValueSet>();
            int org = vs.list.size();
            LinkedList<ValueSet> testPartition = partition(vs);
            //DEBUG
            if (getVsListNum(testPartition) != org){
                System.out.println("测试日志：partition前后数据不一致！！！");
            }
            LinkedList<ValueSet> snapshotforMoveBefor = new LinkedList<ValueSet>();
            snapshotforMoveBefor.addAll(testPartition);

            valueSetLinkedList = toMoveOrMerge(testPartition);

            for (Iterator iterator = valueSetLinkedList.iterator();iterator.hasNext();){
                if (((ValueSet)iterator.next()).list.size() < kValue){
                    System.out.println("<k的值集");
                }
            }

            //DEBUG
            if (getVsListNum(valueSetLinkedList) != org){
 //               try {
 //                   Thread.sleep(5000);
 //               }catch (Exception e){
 //                   e.printStackTrace();
 //               }
                System.out.println("测试日志：MergeOrMove前后数据size不一致！");
            }
            for (Iterator iterator = valueSetLinkedList.iterator();iterator.hasNext();){
                ValueSet tmpVs = (ValueSet)iterator.next();
                if ((tmpVs.qids == 0)/* || (tmpVs.list.size() == kValue)*/){
                    tmpVsList.add(toGenerate(tmpVs,kValue,dValue));
                }else {
                    tmpVsList.addAll(finalPublish(tmpVs));
                }
            }
        }
        return tmpVsList;
    }

    //测试最终划分结果记录数是否正确
    public void getFinalResultValueSetNum(){
        int i = 0;
        for (Iterator iterator= finalResultValueSet.iterator();iterator.hasNext();){
            i += ((ValueSet)iterator.next()).list.size();
        }
        System.out.println("总计："+i+"条记录 | "+"分组："+finalResultValueSet.size());
    }

    public LinkedList<ValueSet> getFinalResultValueSet() {
        return finalResultValueSet;
    }

    //测试最终划分结果是否满足K匿名条件
    public void testFinalResulValueSetKlimit(){
        boolean flag = true;
        for (Iterator iterator = finalResultValueSet.iterator();iterator.hasNext();){
            int i = ((ValueSet)iterator.next()).list.size();
            if (i < kValue){
                System.out.println(i);
                flag = false;
            }
        }
        if (flag){
            System.out.println("K-anonymity Check:Checked！");
        }else {
            System.out.println("K-anonymity Check:Error！");
        }
    }

    //最终最终最终Publish()算法实现
    LinkedList<ValueSet> finalResultValueSet = new LinkedList<ValueSet>();
    public void goPublish(){
        System.out.println("从MySQL读取数据构造表...");
        fillData();
        System.out.println("开始fin1划分...");
        fin1();
        prePublish(root);
        System.out.println("开始fin2划分...");
        for (Iterator iterator = tableValueSet.iterator();iterator.hasNext();){
            finalResultValueSet.addAll(finalPublish((ValueSet)iterator.next()));
//            System.out.println("processing!");
        }
        getFinalResultValueSetNum();
        testFinalResulValueSetKlimit();
        System.out.println("写入文件");
    }

    //把结果写文件
    public void writeResult(File file) throws IOException{
        int row = 0;
        FileWriter writer = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(writer);
        for (Iterator iterator = finalResultValueSet.iterator();iterator.hasNext();){
            bufferedWriter.write("----------------------------------------------" +
                    "------------------------------------");
            bufferedWriter.newLine();
            ValueSet tmpVs = (ValueSet)iterator.next();
            for (Iterator iterator1 = tmpVs.list.iterator();iterator1.hasNext();){
                String rowNodeRecord = ((RowNode)iterator1.next()).toString();
                bufferedWriter.write(rowNodeRecord);
                row ++;
                bufferedWriter.newLine();
            }
            bufferedWriter.write("----------------------------------------------" +
                    "------------------------------------");
            bufferedWriter.newLine();
            bufferedWriter.write(tmpVs.generalizeResult);
            bufferedWriter.newLine();
        }
        System.out.println("写入行数："+row);
    }


    /*
    //最终发布时Publish算法
    public void publish(ValueSet vs){
        if (vs.qids < 0){
            toGenerate(vs,5,5);
        }else {
            LinkedList<ValueSet> valueSetLinkedList = new LinkedList<ValueSet>();
            valueSetLinkedList = partition(vs);
        }
    }

    //测试函数，测试fin1划分结果
    public void testPublish(){
        prePublish(root);
    }
    */

    //概化处理函数，对数值型数值取范围
    public String getRange(ValueSet vs,int qids){
        int min = 1000000, max = 0;
        String range = null;
        switch (qids){
            case 4:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    if (rowNode.education_num > max){
                        max = rowNode.education_num;
                    }else if (rowNode.education_num < min){
                        min = rowNode.education_num;
                    }
                }
                range = new String("["+min+"-"+max+"]");
                break;
            }
            case 6:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    if (rowNode.age > max){
                        max = rowNode.age;
                    }else if (rowNode.age < min){
                        min = rowNode.age;
                    }
                }
                range = new String("["+min+"-"+max+"]");
                break;
            }
            case 7:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    if (rowNode.hours_per_week > max){
                        max = rowNode.hours_per_week;
                    }else if (rowNode.hours_per_week < min){
                        min = rowNode.hours_per_week;
                    }
                }
                range = new String("["+min+"-"+max+"]");
                break;
            }
            case 8:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    if (rowNode.capital_gain > max){
                        max = rowNode.capital_gain;
                    }else if (rowNode.capital_gain < min){
                        min = rowNode.capital_gain;
                    }
                }
                range = new String("["+min+"-"+max+"]");
                break;
            }
        }
        return range;
    }
    //概化处理函数，对字符型数值取集合
    public String getSet(ValueSet vs,int qids){
        HashSet<String> vsSet = new HashSet<String>();
        String result = "{";
        String split = "|";
        switch (qids){
            case 1:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    vsSet.add(rowNode.race);
                }
                break;
            }
            case 2:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    vsSet.add(rowNode.workclass);
                }
                break;
            }
            case 3:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    vsSet.add(rowNode.marital_status);
                }
                break;
            }
            case 5:{
                for (Iterator iterator =  vs.list.iterator();iterator.hasNext();){
                    RowNode rowNode = (RowNode)iterator.next();
                    vsSet.add(rowNode.native_country);
                }
                break;
            }

        }
        for (Iterator iterator = vsSet.iterator();iterator.hasNext();){
            String tmp = (String)iterator.next();
            result = result+tmp+split;
        }
        return result + "}";
    }

    //数据集类
    class ValueSet{
        public LinkedList<RowNode> list = new LinkedList<RowNode>();
        public int qids= 8;
        private String generalizeResult;
        public ValueSet(LinkedList<RowNode> list,int qids){
            this.list = list;
            this.qids = qids;
        }
        public ValueSet(){

        }
    }
    //划分树节点类
    class TreeNode {
        private ValueSet vs = new ValueSet();
        private TreeNode parent = null;
        private LinkedList<TreeNode> child = new LinkedList<TreeNode>();
        private boolean isTurbation=false;

        public TreeNode(){

        }

        public TreeNode(ValueSet vs){
            this.vs = vs;
        }
        public void addRowNode(RowNode rowNode){
            this.vs.list.add(rowNode);
        }

        public void setList(LinkedList<RowNode> list) {
            this.vs.list = list;
        }

        public LinkedList<RowNode> getList() {
            return this.vs.list;
        }

        public int getRowNodeSize(){
            return this.vs.list.size();
        }

        public void setParent(TreeNode parent) {
            this.parent = parent;
        }

        public TreeNode getParent() {
            return parent;
        }

        public void addChild(TreeNode child) {
            this.child.add(child);
        }

        public int getChildSize(){
            return child.size();
        }

        public LinkedList<TreeNode> getChild() {
            return child;
        }
    }

     //按字典序和QIDS秩排序rowLinkedList中的数据
    class RowNodeComparator implements Comparator<RowNode>{
        //race<workclass<maritial_status<education_num<native_country<age<hours_per_week<capital_gain
        public int compare(RowNode r1, RowNode r2){
            if (r1.race.compareTo(r2.race) != 0){
                return r1.race.compareTo(r2.race);
            }else{
                if (r1.workclass.compareTo(r2.workclass) != 0){
                    return r1.workclass.compareTo(r2.workclass);
                }else {
                    if (r1.marital_status.compareTo(r2.marital_status) != 0){
                        return r1.marital_status.compareTo(r2.marital_status);
                    }else {
                        if (r1.education_num != r2.education_num){
                            return r1.education_num - r2.education_num;
                        }else {
                            if (r1.native_country.compareTo(r2.native_country) != 0){
                                return r1.native_country.compareTo(r2.native_country);
                            }else {
                                if (r1.age != r2.age){
                                    return r1.age-r2.age;
                                }else {
                                    if (r1.hours_per_week != r2.hours_per_week){
                                        return r1.hours_per_week-r2.hours_per_week;
                                    }else {
                                        if (r1.capital_gain != r2.capital_gain){
                                            return r1.capital_gain - r2.capital_gain;
                                        }else {
                                            return 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    //行记录对象
    class RowNode{
        private int age;
        private String workclass;
        private int fnlwgt;
        private String education;
        private int education_num;
        private String marital_status;
        private String occupation;
        private String relationship;
        private String race;
        private String sex;
        private int capital_gain;
        private int capital_loss;
        private int hours_per_week;
        private String native_country;
        private boolean income;         // income=>50000 ? true:false;
        private Set<String> sa= new HashSet<String>();
        //race<workclass<maritial_status<education_num<native_country<age<hours_per_week<capital_gain
        private String raceSet;
        private String workclassSet;
        private String maritial_statusSet;
        private String education_numRange;
        private String native_countrySet;
        private String ageRange;
        private String hours_per_weekRange;
        private String capital_gainRange;

        public RowNode(){

        }

        public int getAge() {
            return age;
        }

        public String getWorkclass() {
            return workclass;
        }

        public int getFnlwgt() {
            return fnlwgt;
        }

        public String getEducation() {
            return education;
        }

        public int getEducation_num() {
            return education_num;
        }

        public String getMarital_status() {
            return marital_status;
        }

        public String getOccupation() {
            return occupation;
        }

        public String getRelationship() {
            return relationship;
        }

        public String getRace() {
            return race;
        }

        public String getSex() {
            return sex;
        }

        public int getCapital_gain() {
            return capital_gain;
        }

        public int getCapital_loss() {
            return capital_loss;
        }

        public int getHours_per_week() {
            return hours_per_week;
        }

        public String getNative_country() {
            return native_country;
        }

        public boolean isIncome() {
            return income;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public void setEducation(String education) {
            this.education = education;
        }

        public void setEducation_num(int education_num) {
            this.education_num = education_num;
        }

        public void setFnlwgt(int fnlwgt) {
            this.fnlwgt = fnlwgt;
        }

        public void setMarital_status(String marital_status) {
            this.marital_status = marital_status;
        }

        public void setOccupation(String occupation) {
            this.occupation = occupation;
        }

        public void setRace(String race) {
            this.race = race;
        }

        public void setCapital_gain(int capital_gain) {
            this.capital_gain = capital_gain;
        }

        public void setRelationship(String relationship) {
            this.relationship = relationship;
        }

        public void setCapital_loss(int capital_loss) {
            this.capital_loss = capital_loss;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public void setWorkclass(String workclass) {
            this.workclass = workclass;
        }

        public void setHours_per_week(int hours_per_week) {
            this.hours_per_week = hours_per_week;
        }

        public void setIncome(boolean income) {
            this.income = income;
        }

        public void setNative_country(String native_country) {
            this.native_country = native_country;
        }

        @Override
        public String toString(){
            return new String("{ race:"+raceSet+"\t \tworkclass:"+workclassSet+"\tmaritial_status:"+maritial_statusSet+"\teducation_num:"+education_numRange+"\tnative_country:"+native_countrySet
                    +"\tage:"+ageRange+"\thours_per_week:"+hours_per_weekRange+"\tcapital_gain:"+capital_gainRange+"}");
        }
    }
}
