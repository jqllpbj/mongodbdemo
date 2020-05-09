package com.homolo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;


import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;

import jdk.tools.jaotc.Main;
import org.bson.Document;

import java.sql.SQLOutput;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;

public class Task {
    private MongoDatabase database;

    public Task(MongoDatabase database) {
        this.database = database;
    }

    public static void main(String[] args) throws ParseException {
        MongoClient client=new MongoClient();
        //数据库有则使用,没有则新创建
        MongoDatabase database=client.getDatabase("mydemos");
        Task task=new Task(database);
        task.show();
        client.close();
        System.out.println("1");
        System.out.println("2");


    }

    public void show() throws ParseException {
        /**
         * 1.实现新建数据库,新建集合,新建文档
         */
        //collection有则使用,没有则创建
        MongoCollection<Document> collection=database.getCollection("demo");
        Document document1=new Document("name","jack")
                .append("age",12)
                .append("emial","xxxx@example.com");
        collection.insertOne(document1);

        Document document2=new Document("name","jack")
                .append("age",12)
                .append("emial","xxxx@example.com");

        Document document3=new Document("name","jack")
                .append("age",12)
                .append("emial","xxxx@example.com");

        Document document4=new Document("name","jack")
                .append("age",12)
                .append("emial","xxxx@example.com");
        collection.insertMany(asList(document2,document3,document4));






      /*  *//**
         * 2.查询日期为2019年1月份的数据,并把查询出来的数据"备注"字段的值改为xxx修改
         *//*
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
        Date startdate=dateFormat.parse("2019-1-1");
        Date enddate=dateFormat.parse("2019-1-31");
        collection.updateOne(
                and(gte("createtime",startdate),lte("createtime",enddate)),
                combine(set("remark","jqllpbj")));

        *//**
         * 3.distinct 实现一个功能

         *//*
        collection.distinct("name",String.class);






        *//**
         * 4.aggregation
         *//*
        collection.aggregate(
            asList(
                   match(
                            and(
                                    eq("job","engineer"),
                                    gte("salary",5000),
                                    lte("salary",8000)
                            )),
                    group("$deptment",
                            sum("totalSalary","$salary"),
                            avg("avageSalary","$salary"),
                            max("maxSalary","$salary")
                            ),
                    sort(ascending("name")),
                    skip(2),
                    limit(4)
            )
        );
        *//**
         * 5.index 实现索引的添加和删除
         *//*
        collection.createIndex(Indexes.ascending("name","age"));
        collection.createIndex(
               Indexes.compoundIndex(
                       Indexes.ascending("name","age"),
                       Indexes.descending("salary"))
               );

        collection.dropIndex("name");
        collection.dropIndexes();
        //


        *//**
         *
         * query条件有"不等于",包含,不为空,或的练习
         *//*

        collection.find(
                ne("name","john")
        );
        collection.find(
                ne("age",null)
        );
        collection.find(
                or(
                        in("name","jack","hijh"),
                        gte("salary",598))
        );
        collection.find(
                eq("name","Amarrcof")
        );
        collection.find(
                in("name","jack","han","trsa")
        );
*/

    }



}
