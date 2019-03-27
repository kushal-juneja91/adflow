"""
Simple cli interface to call the python script using arguments

"""
import click
import unittest
import logging
from pyspark.sql import SparkSession
import sys
import os

from adextractutil import AdExtractUtil
from adloadutil import FileSaveUtil
from adtransformutil import AdTransformUtil

@click.group()
@click.version_option(version='0.2.1', prog_name='adanalyzer')
@click.pass_context
def cli(ctx):
    pass


@cli.command(name='analyse', help=' Helps to analyze the ad data and find how user interacted with the ad')
@click.option('-fs', '--filesystem', help='provide the filesystem local or hdfs', type=str, required=True)
@click.option('-input', '--inputfile', help='provide the input file location', type=str, required=True)
@click.option('-output', '--outputfile', help='provide the output file location', type=str, required=True)
@click.option('-format', '--fileformat', help='provide the output file format', type=str, required=True)
@click.pass_obj
def analyse(ctx, filesystem, inputfile, outputfile, fileformat):
    spark = SparkSession.builder.master('local[2]').appName('adflow').enableHiveSupport().getOrCreate()
    print('Starting to process the data')
    input_data = AdExtractUtil().getData(inputfile, filesystem, spark)
    transformed_df=AdTransformUtil().transform(input_data)
    FileSaveUtil().write_data(transformed_df, outputfile, filesystem, fileformat)

def start():
    cli()

if __name__=="__main__":
    start()


