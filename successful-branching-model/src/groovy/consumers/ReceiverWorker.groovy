package consumers

import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class ReceiverWorker implements Runnable {

  DelayConsumer consumer

  volatile boolean running = true

  ReceiverWorker(consumer) {
    this.consumer = consumer
  }

  void run() {
    while (running) {
      List pool = consumer.getFromQueue(50)
      if (pool.size()==0) {
        sleep(1000)
      } else {
        for (msg in pool){ 
          consumer.process(msg)
        }
      }
    }
  }

  void stop() {
    running = false
  }

}