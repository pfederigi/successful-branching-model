package consumers

import grails.converters.JSON

class ConsumerController {

  DelayConsumer consumer = new DelayConsumer()

  def start() {
    consumer.start()
  }

  def stop() {
    consumer.stop()
  }

  def status() {
    def map = [
      "gets": consumer.gets,
      "puts": consumer.puts,
      "process": consumer.process,
      "elements": consumer.q.size()
    ]
    render map as JSON 
  }

}