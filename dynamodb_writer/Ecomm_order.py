import json
import logging
import os
import sys
import requests
from zipfile import ZipFile
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
# snippet-end:[python.example_code.dynamodb.helper.Movies.imports]


# snippet-start:[python.example_code.dynamodb.helper.Movies.class_full]
# snippet-start:[python.example_code.dynamodb.helper.Movies.class_decl]
class Ecomm_order:
    """Encapsulates an Amazon DynamoDB table of movie data."""
    def __init__(self, dyn_resource):
        """
        :param dyn_resource: A Boto3 DynamoDB resource.
        """
        self.dyn_resource = dyn_resource
        self.table = None

    # snippet-start:[python.example_code.dynamodb.DescribeTable]
    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.
        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def add_order(self, uid, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date, order_time):
        """
        Adds a movie to the table.
        :param title: The title of the movie.
        :param year: The release year of the movie.
        :param plot: The plot summary of the movie.
        :param rating: The quality rating of the movie.
        """
        try:
            self.table.put_item(
                Item={
                    'id': uid,
                    'order_time': order_time,
                    'chain': chain,
                    'dept': dept,
                    'category' : category,
                    'company' : company,
                    'brand' : brand,
                    'productsize' : productsize,
                    'productmeasure' : productmeasure,
                    'purchasequantity' : purchasequantity,
                    'purchaseamount' : purchaseamount,
                    'date' : date
                    })
        except ClientError as err:
            logger.error(
                "Couldn't add movie %s to table %s. Here's why: %s: %s",
                title, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

# usage: 
# python3 Ecomm_order.py ecomm-order-table "2013-07-02 00:00:00" "2013-07-03 00:00:00" ./order_all.csv

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("invalid arguments...")

    table_name = sys.argv[1]
    start_time = sys.argv[2]
    end_time = sys.argv[3]
    inputfile = sys.argv[4]

    dyn_resource = boto3.resource('dynamodb')

    ecomm_order = Ecomm_order(dyn_resource)
    table_exists = ecomm_order.exists(table_name)

    if table_exists:
        with open(inputfile, 'r') as data_file:
            for line in data_file:
                uid, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date, order_time = line.split(',')
                if order_time > start_time and order_time < end_time:
                    ecomm_order.add_order(uid, chain, dept, category, company, brand, productsize, productmeasure, purchasequantity, purchaseamount, date, order_time)
