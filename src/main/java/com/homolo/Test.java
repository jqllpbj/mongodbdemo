package com.homolo;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Updates.*;
import static java.util.Arrays.asList;

public class Test {
    public static void main(String[] args) {
        System.out.println("使用git");
        System.out.println("使用git2");
        System.out.println("使用git3");
        System.out.println("使用git4");
        System.out.println("使用git5");

        /**
         * 副本连接操作
         */
        List seeds = new ArrayList();
        ServerAddress address1 = new ServerAddress("192.168.46.133", 27017);
        ServerAddress address2 = new ServerAddress("192.168.46.134", 27017);
        ServerAddress address3 = new ServerAddress("192.168.46.135", 27017);

        seeds.add(address1);
        seeds.add(address2);
        seeds.add(address3);
        MongoClient serverClient = new MongoClient(seeds);
        /**
         * 普通操作
         */

        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mydemos");
        database.createCollection("demo1");
        MongoCollection<Document> collection = database.getCollection("demo");
        /**
         * 插入单个集合
         */
        Document document = new Document("name", "Cafe con leche")
                .append("contact", new Document("phone", "228-555=0149")
                        .append("email", "cafeconle@example.com")
                        .append("location", Arrays.asList(1233, 1111)))
                .append("starts", 3)
                .append("categoruy", Arrays.asList("Backery", "Coffee", "Pastries"));
        collection.insertOne(document);
        /**
         * 插入多个集合
         */
        Document document1 = new Document("name", "Cafe con leche")
                .append("contact", new Document("phone", "228-555=0149")
                        .append("email", "cafeconle@example.com")
                        .append("location", Arrays.asList(1233, 1111)))
                .append("starts", 3)
                .append("categoruy", Arrays.asList("Backery", "Coffee", "Pastries"));
        Document document2 = new Document("name", "Cafe con leche")
                .append("contact", new Document("phone", "228-555=0149")
                        .append("email", "cafeconle@example.com")
                        .append("location", Arrays.asList(1233, 1111)))
                .append("starts", 4)
                .append("categoruy", Arrays.asList("Backery", "Coffee", "Pastries"));
        Document document3 = new Document("name", "Cafe con leche")
                .append("contact", new Document("phone", "228-555=0149")
                        .append("email", "cafeconle@example.com")
                        .append("location", Arrays.asList(1233, 1111)))
                .append("starts", 5)
                .append("categoruy", Arrays.asList("Backery", "Coffee", "Pastries"));
        List<Document> documents = new ArrayList<Document>();
        documents.add(document1);
        documents.add(document2);
        documents.add(document3);
        collection.insertMany(documents);

        collection.insertMany(asList(document2, document3));

        /**
         * 批量操作
         */
        /*
        collection.bulkWrite(
                Arrays.asList(
                        new InsertOneModel<>(new Document("_id",4)),
                        new InsertOneModel<Document>(new Document("_id",5)),
                        new InsertOneModel<Document>(new Document("_id",6)),
                        new UpdateOneModel<>(new Document("_id",1),
                                new Document("$set",new Document("x",2))),
                        new DeleteOneModel<>(new Document("_id",2)),
                        new ReplaceOneModel<>(new Document("_id",3),
                                new Document("_id",3).append("x",4)),
                        new BulkWriteOptions().ordered(true)));

        //unorder
        collection.bulkWrite(
                Arrays.asList(
                        new InsertOneModel<>(new Document("_id",1)),
                        new InsertOneModel<>(new Document("_id",2)),
                        new InsertOneModel<Document>(new Document("_id",3)),
                        new UpdateOneModel<>(new Document("_id",4),
                                new Document("$set",new Document("x",45))),
                        new DeleteOneModel<>(new Document("_id",2)),
                        new ReplaceOneModel<>(new Document("_id",5),
                                new Document("_id",5).append("x",5))),
                new BulkWriteOptions().ordered(false));*/

        /**
         * 更新操作
         *
         */
        collection.updateOne(eq("_id", new ObjectId("5eb3c647713892517525c731")), combine(set("stars", 5)));
        collection.updateOne(
                Filters.eq("_id", new ObjectId("5eb3c647713892517525c732")),
                combine(set("starts", 1),
                        set("contact.phone", "2222"),
                        currentDate("lastModified")
                ));
        collection.updateMany(eq("name", "Cafe con leche"),
                combine(set("starts", 23), currentDate("lastModified"))
                , new UpdateOptions().upsert(true).bypassDocumentValidation(true));

        /**
         * 替换
         */
        collection.replaceOne(eq("_id", new ObjectId("5eb3c647713892517525c732")),
                new Document("name", "Green Salads Buffet")
                        .append("contact", "TBD").append("categories", Arrays.asList("Salads", "Health Foods", "Buffet")));
        collection.replaceOne(eq("_id", new ObjectId("5eb3c647713892517525c731")),
                new Document("starts", "")
                        .append("contact", "TBD")
                        .append("categories", Arrays.asList("Cafe", "Pasttries", "Ice Cream")),
                new UpdateOptions().upsert(true).bypassDocumentValidation(true));

        /**
         * 删除
         */
        collection.deleteOne(eq("_id", new ObjectId("5eb3c5037138924aa26ddb73")));
        collection.deleteMany(eq("starts", 23));

        collection.deleteOne(Filters.eq("likes", 200));
        collection.deleteMany(Filters.eq("like", 200));

        /**
         * 索引
         */
        collection.createIndex(Indexes.ascending("starts"));
        collection.createIndex(Indexes.descending("starts", "name"));
        collection.createIndex(Indexes.compoundIndex(Indexes.descending("name"), Indexes.ascending("starts")));
        collection.createIndex(Indexes.text("contact"));
        collection.createIndex(Indexes.hashed("_id"));
        collection.createIndex(Indexes.geo2dsphere("contact.location"));
        //删除索引
        collection.dropIndex("name");
        collection.dropIndexes();


        /**
         * 查询
         */
        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(Document document) {
                System.out.println(document.toJson());
            }
        };
       /*
        Block<Document> o=(document5)->
            System.out.println(document5.toJson());*/

        collection.find().forEach(printBlock);
        collection.find(eq("name", "Green Salads Buffet")).forEach(printBlock);



        /*collection.find(new Document("stars",
                new Document("$gt", 2).append("$lte", 5))
                .append("categories", "Cafe")
        ).forEach(printBlock);*/
        collection.find(
                and(
                        gte("stars", 2),
                        lte("stars", 5),
                        eq("categories", "Italian")
                )
        ).forEach(printBlock);

       /* collection.find(
                and(
                    gte("stars",2),
                    lte("stars",5),
                    eq("categories")
                )
        ).projection(
                new Document("name",1).append("stars",1).append("categories",1).append("_id",0)
        ).forEach(printBlock);*/
        collection.find(
                and(
                        gte("stars", 56),
                        lte("stars", 78),
                        eq("categories", "Pasta")
                )
        ).projection(
                fields(
                    include("name", "stars", "categories"),
                    excludeId()
                )
        ).forEach(printBlock);


        collection.find(
                and(
                    gte("stars", 56),
                    lte("stars", 78),
                    eq("categories", "Pizzeria")
                )
        ).sort(Sorts.ascending("name")
        ).forEach(printBlock);



        FindIterable<Document> findIterable = collection.find(
                and(
                gte("stars", 56),
                lte("stars", 78),
                eq("categories", "Pasta")
                )
        ).sort(Sorts.ascending("name")
        ).projection(fields(include("name", "stars", "categories"), excludeId()));


        /**
         * 遍历集合
         */
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            System.out.println(mongoCursor.next());
        }


        /**
         * 聚合函数
         */

        collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("categories","Backery")),
                                Aggregates.group("$stars", sum("count",1))
                )
        ).forEach(printBlock);


        collection.aggregate(
                Arrays.asList(
                   Aggregates.project(
                          Projections.fields(
                                  Projections.excludeId(),
                                  Projections.include("name"),
                                  Projections.computed(
                                          "firstCategory",
                                          new Document("$arrayElement",Arrays.asList("$categories",0))
                                  )
                          )
                   )
                )).forEach(printBlock);


        collection.aggregate(asList(
                match(and(eq("owner", "Tom"), gt("words", 500)))
        ));
        collection.aggregate(asList(
                match(in("ower", "tom", "john", "mike")),
                group("$owner",
                        sum("totalWords", "$words"),
                        avg("averageWords", "$words"),
                        Accumulators.max("maxWords", "$words")
                )
        ));

        collection.aggregate(
                Arrays.asList(sample(3))
        );

        collection.aggregate(
                Arrays.asList(
                        sample(3),
                        project(fields(
                                include("title", "owner"),
                                excludeId()
                        ))
                )
        );

        collection.aggregate(
                asList(
                        skip(1),
                        limit(2),
                        project(fields(
                                include("title", "ownere"),
                                excludeId()
                        ))
                )
        );


        //join

        collection.insertMany(
                asList(
                        new Document("writer", "tom").append("score", 100),
                        new Document("writer", "joe").append("score", 95),
                        new Document("writer", "john").append("score", 80)
                )
        );
        MongoCollection<Document> sore = database.getCollection("score");
        collection.aggregate(
                asList(
                        lookup("score", "owner", "writer", "joinedOutput")
                )
        );

        collection.aggregate(
                asList(
                        match(size("comments", 2)),
                        project(fields(
                                include("comments"),
                                excludeId()
                        ))
                )
        );

        collection.distinct("owner", String.class);


    }

}
