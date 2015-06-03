package consumers

import java.util.UUID
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class SenderWorker implements Runnable {

  DelayConsumer consumer

  volatile boolean running = true

  SenderWorker(consumer) {
    this.consumer = consumer
  }

  void run() {
    while (running) {
      UUID idOne = UUID.randomUUID();
      def data = ["id": idOne, "time": System.currentTimeMillis()]
      consumer.putToQueue(data)
      sleep(100)
    }
  }

  void stop() {
    running = false
  }

}