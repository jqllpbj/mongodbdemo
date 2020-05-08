package com.homolo;


import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import static com.mongodb.client.model.Filters.*;



public class MongodbTest {
    /*public static void main(String[] args) throws ParseException {
        String remark = "你好世界";
        //创建客户端
        MongoClient mongoClient = new MongoClient();
        //连接数据库
        MongoDatabase database = mongoClient.getDatabase("demo");
        //连接集合
        MongoCollection<Document> collection = database.getCollection("user");

        

        //插入文档
        insertDocument(collection);
        System.out.println("--------------------------------");

        //查找日期为2019/01的文档并修改其备注

        queryAndUpdate(collection, remark);


        System.out.println("--------------------------------");
        //去重,查找所有不一样的姓名
        getDistinct(collection);

        System.out.println("--------------------------------");
        //聚合
        testAggregation(collection);

        System.out.println("--------------------------------");
        //添加索引
        createNewIndex(collection);

        System.out.println("--------------------------------");
        //删除索引
        deleteIndex(collection);

        System.out.println("--------------------------------");
        //条件查询
        queryByCondition(collection, remark);

        //关闭链接
        mongoClient.close();
    }

    *//**
     * 条件查询
     *
     * @param collection
     * @param remark
     *//*
    private static void queryByCondition(MongoCollection<Document> collection, String remark) {
        System.out.println("年龄不为12且名字不为空的记录：");
        FindIterable<Document> documents = collection.find(
                and(ne("age", 12)
                        , ne("name", "")
                ));

        for (Document document : documents) {
            System.out.println(document);
        }

        System.out.println("年龄为12或remark包含???的记录：");
        FindIterable<Document> documents1 = collection.find(or(eq("age", 12), in("remark", remark)));
        for (Document document : documents1) {
            System.out.println(document);
        }
        System.out.println("查询结束");
    }

    *//**
     * 查询并更新
     *
     * @param collection
     * @param remark
     * @throws ParseException
     *//*
    private static void queryAndUpdate(MongoCollection<Document> collection, String remark) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date startTime = format.parse("2019-01-01");
        Date endTime = format.parse("2019-01-31");
        FindIterable<Document> documents = collection.find(and(gte("createTime", startTime)
                , lte("createTime", endTime)));
        System.out.println("日期为2019年1月份的数据为：");
        for (Document document : documents) {
            System.out.println(document);
        }

        collection.updateMany(and(gte("createTime", startTime)
                , lte("createTime", endTime)), new Document("$set", new Document("remark", remark)));
        System.out.println("更新成功！！！");
    }

    *//**
     * 测试聚合
     *
     * @param collection
     *//*
    private static void testAggregation(MongoCollection<Document> collection) {
        System.out.println("聚合name为nancy的记录：");
        AggregateIterable<Document> documents = collection.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("name", "nancy"))));
        for (Document document : documents) {
            System.out.println(document);
        }

    }

    *//**
     * 去重
     *
     * @param collection
     *//*
    private static void getDistinct(MongoCollection<Document> collection) {
        DistinctIterable<String> strings = collection.distinct("name", String.class);
        System.out.println("去重，输出所有不同姓名：");
        for (String string : strings) {
            System.out.println(string);
        }
        System.out.println("查找成功");
    }

    *//**
     * 删除索引
     *
     * @param collection
     *//*
    private static void deleteIndex(MongoCollection<Document> collection) {
        collection.dropIndex("age_1");
        System.out.println("删除索引成功");
    }

    *//**
     * 创建新索引
     *
     * @param collection
     *//*
    private static void createNewIndex(MongoCollection<Document> collection) {
        collection.createIndex(new Document("age", 1));
        System.out.println("创建索引成功");
    }

    *//**
     * 插入文档
     *
     * @param collection
     * @throws ParseException
     *//*
    private static void insertDocument(MongoCollection<Document> collection) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = format.parse("2019-02-22");
        Document document = new Document("name", "punch")
                .append("age", 22)
                .append("remark", "重击")
                .append("createTime", date);
        collection.insertOne(document);
        System.out.println("插入成功");
    }*/
}
