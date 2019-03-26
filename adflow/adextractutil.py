class AdExtractUtil():
    def getData(self, filelocation, filesystem, spark):
        if filesystem != 'hdfs':
            filelocation = "file:///" + filelocation
        adsDf = spark.read.json(filelocation)
        return adsDf
