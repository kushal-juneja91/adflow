class AdExtractUtil():
    def getData(self, filelocation, filesystem, spark):
        if filesystem=='local':
            filelocation = "file://"+filelocation
        if filesystem=='hdfs':
            filelocation=filelocation
        adsDf = spark.read.json(filelocation)
        return adsDf
