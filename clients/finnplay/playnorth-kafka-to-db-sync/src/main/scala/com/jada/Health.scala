package com.jada.betway

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.kafka.streams.KafkaStreams

import java.io.IOException
import java.net.InetSocketAddress

class Health(port: Int) {
  private var server: HttpServer = _

  private val OK = 200
  private val ERROR = 500

  def start(): Unit = {
    try
      server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0)
    catch {
      case ioe: IOException =>
        throw new RuntimeException("Could not setup http server: ", ioe)
    }
    server.createContext("/health", new Root)
    server.start()
  }

  private class Root extends HttpHandler {
    def handle(exchange: HttpExchange) = {
      //if the input don't exists beforehand, the stream app won't start,
      //but we don't want the deployment to fail when in staging
      val responseCode = OK
      exchange.sendResponseHeaders(responseCode, 0)
      exchange.close()
    }
  }

  def stop: Unit = {
    server.stop(0)
  }
}
