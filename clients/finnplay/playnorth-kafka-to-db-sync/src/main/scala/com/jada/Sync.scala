package com.jada

import com.jada.betway.Health
import com.jada.betway.model.StoreData
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import java.sql.DriverManager
import java.util.Properties

object Sync extends App{

private val logger=LoggerFactory.getLogger("com.jada.betway.Sync")

        Thread.setDefaultUncaughtExceptionHandler(
        new Thread.UncaughtExceptionHandler(){
        def uncaughtException(t:Thread,e:Throwable){
        logger.error(s"Uncaught exception",e)
        }
        }
        )

        val props=new Properties()
        props.put("bootstrap.servers", "Finnplay-kafka-0.internal.cloudapp.net:19092,Finnplay-kafka-1.internal.cloudapp.net:19092,Finnplay-kafka-2.internal.cloudapp.net:19092")
        props.put(
        "key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer"
        )
        props.put(
        "value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer"
        )
        props.put("auto.offset.reset","earliest")
        props.put("group.id","sync")

        import com.ovoenergy.kafka.serialization.circe._
        import com.ovoenergy.kafka.serialization.core._
import scala.collection.JavaConverters._

        // Import the Circe generic support
        import io.circe.generic.auto._
        import io.circe.syntax._

        val consumer:KafkaConsumer[String,StoreData]=
        new KafkaConsumer(props, StringDeserializer, circeJsonDeserializer[StoreData])
        consumer.subscribe(java.util.Arrays.asList("finnplay-processor-v3-players-store-changelog"))
        while(true){
        val record=consumer.poll(1000).asScala
        for(data<-record.iterator){
        if(data.value()!=null){
                try {
                        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")
                } catch {
                        case e: Exception => throw new RuntimeException("Could not load Snowflake JDBC driver", e)
                }

                val connProps = new Properties()
                connProps.put("user", "APP_MIDDLEWARE")
                connProps.put("password", System.getenv("SF_PASSWORD"))
                connProps.put("account", account)
                connProps.put("ssl", "off")

                val connectionUrl = "jdbc:snowflake://irzzdfb-bj65063.snowflakecomputing.com"
                try {
                        val conn = DriverManager.getConnection(connectionUrl, props)
                        val sql =
                                s"""
                                   |INSERT INTO bus.middleware_playnorth_history (
                                   |test, test2
                                   |) VALUES
                                   | ('${value.player_id.getOrElse("")}', '${value.reg_datetime.getOrElse("")}');
                                   |""".stripMargin
                        val ps = conn.prepareStatement(sql)
                        ps.execute()
                        logger.info(sql)
                        conn.close()
                } catch {
                        case e: Exception => throw new RuntimeException("Exception when writing to database", e)
                }

        }
        }
        }
        consumer.commitAsync()
        }

        val health=new Health(80)
        health.start()

        sys.addShutdownHook{
        try{
        health.stop
        }catch{
        case _:Exception=>
        }
        }
        }
