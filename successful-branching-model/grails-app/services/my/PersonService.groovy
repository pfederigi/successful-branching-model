package my

class PersonService {

    def getAll() {
        log.trace("trace getAll")
        log.debug("debug getAll")
        log.info("info getAll")
        log.warn("warn getAll")
        log.error("error getAll")

        println "console getAll"
    }

}