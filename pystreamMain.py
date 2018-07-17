import pystreamer as ps
import pydocumentdb;
import pydocumentdb.document_client as document_client
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
import pandas as pd
import uuid
import subprocess
import os
import pyodbc


# settings
# get the publication id and publisher key from user

#publicationId = '6b1f5ea3-e89c-495d-8064-9de705f73ca5'  #publication id : what is being published
#publisherKey = '40a2bedc-3797-40d4-a007-645b6b5bbed3'   #publisher key : who is publishing it
#subscriptionId = '6b1f5ea3-e89c-495d-8064-9de705f73ca5'
#subscriberKey = '40a2bedc-3797-40d4-a007-645b6b5bbed3'
#configServer = 'localhost'
#configDB = 'dataControl'

#get the metadata of the publicationId id from publishers json file
# based on the type, it will return the storage accounts or cosmos db
# result will be a dictionary

def publish_data (publicationId,publisherKey,configServer,configDB):
    subscription_name = ''
    subscription_type = ''
    publication_name = ''
    publication_type = ''
    container = ''
    account_name = ''
    account_key = ''
    endpoint = ''
    masterkey = ''
    database_id = ''
    collection_id = ''

    sm = pd.read_json('publishermeta.json').to_dict()
    if (publisherKey in sm):
        dj =  pd.read_json('publishers.json')
        rs = ps.get_publication_metadata(publicationId,publisherKey,dj)
        for keys in rs:
            publication_name = rs[keys]['name']
            publication_type = rs[keys]['type']
            if(publication_type == 'documentDB'):
                database_id = rs[keys]['targetDB']
                collection_id = rs[keys]['targetCollection']
                masterkey = rs[keys]['key']
                endpoint = rs[keys]['endpoint']
                print('\n---------------------------------------------')
                print('Publication: ' + publication_name)
                print('Database: ' + database_id)
                print('Collection: ' + collection_id)
                print('---------------------------------------------')
                database_link = 'dbs/' + database_id
                collection_link = database_link + '/colls/' + collection_id
                client = document_client.DocumentClient(endpoint, {'masterKey': masterkey})
                jobsource = pd.read_json('upload_jobs.json')

                params = {
                "publicationId":publicationId,
                "jobsource":jobsource,
                "database_id":database_id,
                "database_link":database_link,
                "collection_id":collection_id,
                "collection_link":collection_link,
                "key":masterkey,
                "endpoint":endpoint,
                "client":client
                }

                ps.execute_publication_documentDB_jobs(params)
                maxTimeStamp = ps.get_max_timestamp_from_collection(database_link,collection_id,collection_link,client)
                print('-----------------------------------------------------------------------------------------------------------')
                cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                      "Server="+configServer+";"
                                      "Database="+configDB+";"
                                      "Trusted_Connection=yes;",autocommit=True)
                config_data = ps.set_sql_publication_config_data(publicationId,maxTimeStamp,cnxn)
                print(config_data)


def subscribe_data(subscriptionId,subscriberKey,configServer,configDB):
    subscription_name = ''
    subscription_type = ''
    publication_name = ''
    publication_type = ''
    container = ''
    account_name = ''
    account_key = ''
    endpoint = ''
    masterkey = ''
    database_id = ''
    collection_id = ''
    targetStageTable = ''
    targetDeleteTable = ''
    targetSchema = ''
    targetStageSchema = ''
    #client = document_client.DocumentClient(endpoint, {'masterKey': masterkey})
    subs =  pd.read_json('subscribermeta.json').to_dict()
    if(subscriberKey in subs):
        dj = pd.read_json('subscribers.json')
        rs = ps.get_subscription_metadata(subscriptionId,subscriberKey,dj)
        for keys in rs:
            subscription_name = rs[keys]['name']
            subscription_type = rs[keys]['type']
            if(subscription_type =='sql_download'):
                targetServer = rs[keys]['targetServer']
                targetDB = rs[keys]['targetDB']
                targetTable = rs[keys]['targetTable']
                database_id = rs[keys]['source_database']
                collection_id = rs[keys]['source_collection']
                masterkey = rs[keys]['key']
                endpoint = rs[keys]['endpoint']
                targetStageTable = rs[keys]['targetStageTable']
                targetDeleteTable = rs[keys]['targetDeleteTable']
                targetSchema = rs[keys]['targetSchema']
                targetStageSchema = rs[keys]['targetStageSchema']
                client = document_client.DocumentClient(endpoint, {'masterKey': masterkey})

                print('\n---------------------------------------------')
                print('subscription: ' + subscription_name)
                print('Database: ' + database_id)
                print('Collection: ' + collection_id)
                print('---------------------------------------------')
                database_link = 'dbs/' + database_id
                collection_link = database_link + '/colls/' + collection_id
                cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                      "Server="+configServer+";"
                                      "Database="+configDB+";"
                                      "Trusted_Connection=yes;",autocommit=True)
                last_import_timeStamp = ps.get_max_timestamp_from_sql_database(subscriptionId,cnxn)

                params = {
                "subscriptionId":subscriptionId,
                "database_id":database_id,
                "collection_id":collection_id,
                "collection_link":collection_link,
                "key":masterkey,
                "endpoint":endpoint,
                "client":client,
                "targetServer":targetServer,
                "targetDB":targetDB,
                "targetTable":targetTable,
                "last_import_timeStamp":last_import_timeStamp,
                "targetStageTable":targetStageTable,
                "targetDeleteTable":targetDeleteTable,
                "targetSchema":targetSchema,
                "targetStageSchema":targetStageSchema
                }

                maxTime = ps.execute_subscription_documentDB_jobs(params)
                config_data = ps.set_sql_subscription_config_data(subscriptionId,maxTime,cnxn)
                print(last_import_timeStamp)



#publish_data(publicationId,publisherKey,configServer,configDB)
#subscribe_data(subscriptionId,subscriberKey,configServer,configDB)
