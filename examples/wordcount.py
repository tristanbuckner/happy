import sys, happy, happy.log

happy.log.setLevel("info")
log = happy.log.getLogger("wordcount")

class WordCount(happy.HappyJob):
    def __init__(self, inputpath, outputpath):
        happy.HappyJob.__init__(self)
        self.inputpaths = inputpath
        self.outputpath = outputpath
        self.inputformat = "text"

    def map(self, records, task):
        for _, value in records:
            for word in value.split():
                task.collect(word, "1")

    def reduce(self, key, values, task):
        count = 0;
        for _ in values: count += 1
        task.collect(key, str(count))
        log.debug(key + ":" + str(count))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Usage: <inputpath> <outputpath>"
        sys.exit(-1)
    wc = WordCount(sys.argv[1], sys.argv[2])
    wc.run()
