package consumers

import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class DelayConsumer {

  static final int RECEIVERS = 1
  static final int SENDERS = 1

  ExecutorService service
  def workers = []
  DelayQueue q = new DelayQueue()

  int gets = 0
  int puts = 0
  int process = 0

  def start() {
    workers = []
    service = Executors.newFixedThreadPool(RECEIVERS+SENDERS)
    SENDERS.times {
      def worker = new SenderWorker(this)
      workers << worker
      service.execute(worker)
    }
    RECEIVERS.times {  
      def worker = new ReceiverWorker(this)
      workers << worker
      service.execute(worker)
    }
    println "${workers.size()} workers started"
  }

  def process(delayedMessage) {
    def data = delayedMessage.message
    long waitTime = System.currentTimeMillis() - delayedMessage.origin
    println "process [waitTime: $waitTime, data: ${data}]"
    process++
  }

  List getFromQueue(int limit) {
    println "Trying to get an element [size: ${q.size()}]"
    List pool = []
    q.drainTo(pool, limit)
    gets++
    println "Drained ${pool.size()} elements"
    return pool
  }

  def putToQueue(data) {
    q.offer(new DelayedMessage(5000, data.id, data))
    puts++
    println "PUT [size: ${q.size()}, data: ${data}]"
  }

  def stop() {
    println "Finalizando"
    for (w in workers) {
      if (w instanceof SenderWorker)
        w.stop()
    }
    while (q.size()>0) {
      sleep(10000)
    }
    for (w in workers) {
        w.stop()
    }
    service.shutdown()
    service.awaitTermination(10000, TimeUnit.MILLISECONDS)
    println "Finalizado"
  }

}