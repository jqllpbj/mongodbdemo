package com.homolo;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;



import com.mongodb.client.model.Indexes;




import org.bson.Document;



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
        System.out.println("122");

    }

    public void show() throws ParseException {
        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(Document document) {
                System.out.println(document.toJson());
            }
        };

        /**
         * 1.实现新建数据库,新建集合,新建文档
         */
        //collection有则使用,没有则创建
        MongoCollection<Document> collection=database.getCollection("demo");
        Document document1=new Document("name","jack")
                .append("job","engineer")
                .append("birth",new SimpleDateFormat("yyyy-M-dd").parse("1995-1-23"))
                .append("dept","1")
                .append("salary",5000)
                .append("remark","");
        collection.insertOne(document1);
        collection.find().forEach(printBlock);
        System.out.println("--------------------------------");

        Document document2=new Document("name","joh")
                .append("job","engineer")
                .append("birth",new SimpleDateFormat("yyyy-M-dd").parse("1995-1-24"))
                .append("job","engineer")
                .append("dept","1")
                .append("salary",7000)
                .append("remark","");

        Document document3=new Document("name","james")
                .append("job","engineer")
                .append("birth",new SimpleDateFormat("yyyy-M-dd").parse("1995-1-25"))
                .append("job","engineer")
                .append("dept","2")
                .append("salary",6000)
                .append("remark","");

        Document document4=new Document("name","jane")
                .append("job","engineer")
                .append("birth",new SimpleDateFormat("yyyy-M-dd").parse("1995-1-26"))
                .append("job","engineer")
                .append("dept","2")
                .append("salary",1500)
                .append("remark","");
        collection.insertMany(asList(document2,document3,document4));
        collection.find().forEach(printBlock);

        System.out.println("1--------------------------------");


        /**
         * 2.查询日期为2019年1月份的数据,并把查询出来的数据"备注"字段的值改为xxx修改
         */

        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
        Date startdate=dateFormat.parse("2019-1-1");
        Date enddate=dateFormat.parse("2019-1-31");
        collection.updateOne(
                and(gte("birth",startdate),lte("birth",enddate)),
                combine(set("remark","jqllpbj")));
        collection.find().forEach(printBlock);
        System.out.println("2--------------------------------");



        /**
         *  3.distinct 实现一个功能
         */

        collection.distinct("name",String.class);
        collection.find().forEach(printBlock);
        System.out.println("3--------------------------------");


        /**
         * 4.aggregation
         */
        collection.aggregate(
            asList(
                   match(
                            and(
                                    eq("job","engineer"),
                                    gte("salary",5000),
                                    lte("salary",8000)
                            )),
                    group("$dept",
                            sum("totalSalary","$salary"),
                            avg("avageSalary","$salary"),
                            max("maxSalary","$salary")
                            ),
                    sort(ascending("name")),
                    skip(0),
                    limit(4)
            )
        ).forEach(printBlock);
        System.out.println("4--------------------------------");
        /**
         * 5.index 实现索引的添加和删除
         */

        collection.createIndex(Indexes.ascending("name","age"));
        collection.createIndex(
               Indexes.compoundIndex(
                       Indexes.ascending("name","age"),
                       Indexes.descending("salary"))
               );

        //collection.dropIndex("name");
        collection.dropIndexes();
        //


        /**
         * 6.query条件有"不等于",包含,不为空,或的练习
         *
         */
        collection.find(
                and(
                        ne("name",""),
                        ne("name",null)
                )
        ).forEach(printBlock);
        System.out.println("5--------------------------------");
        collection.find(in("name","jack")).forEach(printBlock);
        System.out.println("6-------------------------------");


       /* collection.find(
                ne("name","john")
        ).forEach(printBlock);
        collection.find(
                ne("age",null)
        ).forEach(printBlock);
        collection.find(
                or(
                        in("name","jack","hijh"),
                        gte("salary",598))
        ).forEach(printBlock);
        collection.find(
                eq("name","Amarrcof")
        );
        collection.find(
                in("name","jack","han","trsa")
        );*/

    }



}
