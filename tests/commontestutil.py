import unittest
import logging
from pyspark.sql import SparkSession
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from adflow.adextractutil import AdExtractUtil
from adflow.adtransformutil import AdTransformUtil
from adflow.adloadutil import FileSaveUtil


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                            .master('local[2]')
                            .appName('adflow')
                            .enableHiveSupport()
                            .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        cls.adextract= AdExtractUtil()
        cls.adtransform =  AdTransformUtil()
        cls.filesave = FileSaveUtil()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
