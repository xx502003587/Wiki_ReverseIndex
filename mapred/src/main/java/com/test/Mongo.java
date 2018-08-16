package com.test;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import org.apache.avro.generic.GenericData;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class Mongo {
    public static String t = "d";
    public static void main( String args[] ){
        try{
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "192.168.1.198" , 27017 );

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("wiki");
            MongoCollection<Document> collection = mongoDatabase.getCollection("search");

//            List<String> list = new ArrayList<String>();
//            list.add("a");
//            list.add("b");
//            list.add("c");
//            list.add("d");
//
//            Document document = new Document("title", "MongoDB").
//                    append("description", "database").
//                    append("likes", 100).
//                    append("by", list);
//
//            //List<Document> documents = new ArrayList<Document>();
//            //documents.add(document);
//            //collection.insertMany(documents);
//            collection.insertOne(document);
//            System.out.println("文档插入成功");

            //检索所有文档
            /**
             * 1. 获取迭代器FindIterable<Document>
             * 2. 获取游标MongoCursor<Document>
             * 3. 通过游标遍历检索出的文档集合
             * */
            FindIterable<Document> findIterable = collection.find();
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            while(mongoCursor.hasNext()){
                //System.out.println(mongoCursor.next());
                Object id = mongoCursor.next().get("_id");

                Document myDoc = collection.find(Filters.eq("_id", id)).first();
                collection.deleteOne(myDoc);

            }



        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }



}
