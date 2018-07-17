import pydocumentdb
import pandas as pd
import uuid
import pydocumentdb.document_client as document_client
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
import subprocess
import os
import time
import sqlalchemy as sa
import pyodbc
import numpy as np

def get_file_list_from_container(container,account_name,account_key):
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
    generator = block_blob_service.list_blobs(container)
    for blob in generator:
        print (blob.name, blob.properties.last_modified)

def upload_csv_file_to_blob(container,account_name,account_key,targetfile,sourcefile):
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
#Upload the CSV file to Azure
    print ('Uploading file..')
    try:
        block_blob_service.create_blob_from_path(
            container,
            targetfile,
            sourcefile,
            content_settings=ContentSettings(content_type='application/CSV')
                    )
        print('Upload Complete!')
    except Exception as e:
        print('Error: '+ str(e))

def upsert_cosmos_document(database_link,collection_link,data,client):
    try:
        client.UpsertDocument(collection_link,data,options=None)
    except Exception as e:
        print('Error: '+ str(e))

def read_cosmos_documents_from_collection(collection_link,record_limit,client):
    try:
        if(record_limit <= 0):
            print(list(client.ReadDocuments(collection_link)))
        else:
            print(list(client.ReadDocuments(collection_link, {'maxItemCount':record_limit})))
    except Exception as e:
        print('Error: '+ str(e))

def upload_dataframe_to_cosmos_collection(database_link,collection_link,dataset,client):
    try:
        data_dict_list = dataset.to_dict('records')
        num_records = len(data_dict_list)
        for i in range(num_records):
            data_dict = data_dict_list[i]
            upsert_cosmos_document(database_link,collection_link,data_dict,client)
    except Exception as e:
        print('Error: '+ str(e))

def get_max_timestamp_from_collection(database_link,collectionName,collection_link,client):
    maxTimeStamp = 0
    querySQL = "SELECT top 1 s._ts FROM " + collectionName + " as s order by s._ts desc"
    query = {"query": querySQL}
    result_iterable = client.QueryDocuments(collection_link, query)
    results = list(result_iterable);
    if(len(results) > 0):
        return (results[0]['_ts'])
    else:
        return (0)


def get_publication_metadata(publicationId,publisherKey, data):
    dj = data.T
    if(dj.loc[publicationId]['publisherKey'] == publisherKey):
        return(dj.loc[publicationId]['publication'])
    else:
        return (-1)

def get_subscription_metadata(subscriptionId,subscriberKey, data):
    dj = data.T
    if(dj.loc[subscriptionId]['subscriberKey'] == subscriberKey):
        return(dj.loc[subscriptionId]['subscription'])
    else:
        return (-1)

def execute_publication_documentDB_jobs (params):
    publicationId = params['publicationId']
    data = params['jobsource']
    database_id = params['database_id']
    database_link = params['database_link']
    collection_id = params['collection_id']
    collection_link = params['collection_link']
    key = params['key']
    endpoint = params['endpoint']
    client = params['client']

    jobsource = data.T # configurations
    jobsource = jobsource[jobsource['publicationId'] == publicationId]
    jobsource = jobsource.to_dict('index')
    for jobkeys in jobsource:
        jobtype = jobsource[jobkeys]['jobType']
        if(jobtype =='sql_upload'):
            source_server = jobsource[jobkeys]['source_server']
            source_database = jobsource[jobkeys]['source_database']
            data_source_query = jobsource[jobkeys]['data_source_query']
            id_query = jobsource[jobkeys]['id_query']
            authentication = jobsource[jobkeys]['authentication']

            # create a batch file with command to upload data from SQL server to cosmos db
            if (authentication =='trusted'):
                conn_string = 'Data Source='+source_server+';Initial Catalog='+source_database+';Trusted_Connection=yes'
                comm = 'dt.exe /s:SQL /s.ConnectionString:"'+conn_string+'" /s.Query:"'+data_source_query+'" /t:DocumentDB /t.ConnectionString:AccountEndpoint='+ endpoint+';AccountKey='+key+';Database='+database_id+' /t.IdField:Id /t.DisableIdGeneration /t.UpdateExisting /t.Collection:'+collection_id
                batfile = 'uploader-'+collection_id+'.bat'
                batfileFolder = 'azure_data/'
                batfileName = batfileFolder+batfile
                # create batch file with commands to upload data to cosmos db
                with open(batfileName, 'w') as f:
                    f.write(comm)

                # get previously uploaded Ids from cosmos
                prev_ids = get_id_from_collection(client, collection_link, collection_id)
                prev_ids_df = pd.DataFrame(prev_ids)
                prev_ids_df['id'] = prev_ids_df['id'].apply(str)


                # execute the batch file to upload data to cosmos
                project_root = os.path.abspath(os.path.dirname(__file__))
                definition_root = os.path.join(project_root, 'azure_data',batfile)
                process=subprocess.Popen([definition_root],cwd='azure_data') # cwd: sets the current working directory
                process.wait()
                cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                      "Server="+source_server+";"
                                      "Database="+source_database+";"
                                      "Trusted_Connection=yes;")

                # get the current ids from SQL db
                current_ids = get_id_from_sql_database(id_query,cnxn)
                current_ids['id'] =  current_ids['id'].apply(str)
                current_ids = current_ids['id']

                # find the ids that are in Cosmos but not in source SQL table
                diff = prev_ids_df[~prev_ids_df.id.isin(current_ids)]
                msg = str(len(diff)) + ' item(s) will be deleted....'
                deletes = list(diff['id'])
                print (msg)

                # delete the documents from cosmos
                for i in range(len(deletes)):
                    doc_id = deletes[i]
                    docLink = collection_link+'/docs/'+ str(doc_id)
                    client.DeleteDocument(docLink,options=None)

                # TO DO: add logic to upload full table if delete nbr exceeds a record_limit

def set_sql_publication_config_data(publicationId,maxTimeStamp,cnxn):
    data_source_query = "exec usp_update_PublicationUploadConfig " + "'"+publicationId+"',"+ str(maxTimeStamp)
    config_data = pd.read_sql(data_source_query, cnxn)
    return config_data


def get_id_from_sql_database(query,cnxn):
    data = pd.read_sql(query, cnxn)
    return data

def get_id_from_collection(client, collection_link, collectionName):
    querySQL = "SELECT  s.id FROM " + collectionName + " as s "
    query = {"query": querySQL}
    results =  QueryDocumentsWithCustomQuery(client, collection_link, query)
    return (results)


def QueryDocumentsWithCustomQuery(client, collection_link, query):
    try:
        results = list(client.QueryDocuments(collection_link, query))
        return results
    except errors.DocumentDBError as e:
        if e.status_code == 404:
            print("Document doesn't exist")
        elif e.status_code == 400:
            # Can occur when we are trying to query on excluded paths
            print("Bad Request exception occured: ", e)
            pass
        else:
            raise
    finally:
        print()


def execute_subscription_documentDB_jobs (params):
    subscriptionId = params['subscriptionId']
    database_id = params['database_id']
    collection_id = params['collection_id']
    collection_link = params['collection_link']
    key = params['key']
    endpoint = params['endpoint']
    client = params['client']
    targetServer = params['targetServer']
    targetDB = params['targetDB']
    targetTable = params['targetTable']
    last_import_timeStamp = params['last_import_timeStamp']
    targetStageTable = params['targetStageTable']
    targetDeleteTable = params['targetDeleteTable']
    targetSchema = params['targetSchema']
    targetStageSchema = params['targetStageSchema']

    # create batch file with commands to download data from azure to local directory
    # dont execute it before getting the ids from the existing data in sql table.

    output_file_name = 'output-'+collection_id+'.json'
    azure_conn_string = 'AccountEndpoint='+endpoint+';AccountKey='+key+';Database='+database_id
    query = 'select * from '+collection_id+' as tr  where tr._ts > ' + str(last_import_timeStamp)
    comm = 'dt.exe /s:DocumentDB /s.ConnectionString:'+azure_conn_string+' /s.Collection:'+collection_id+' /s.InternalFields /s.Query:"'+query+'"  /t:JsonFile /t.File:'+output_file_name+' /t.Overwrite'
    batfile = 'downloader-'+collection_id+'.bat'
    batfileFolder = 'azure_data/'
    batfileName = batfileFolder+batfile
    with open(batfileName, 'w') as f:
        f.write(comm)

    # sqlalchemy engine gives us ability to bulk insert data in table with configurable chunk size.
    eng = 'mssql+pyodbc://'+targetServer+'/'+targetDB+'?driver=SQL+Server+Native+Client+11.0'
    engine = sa.create_engine(eng)

    # ids that are in cloud (source data)
    cosmos_ids = get_id_from_collection(client, collection_link, collection_id)
    cosmos_ids_df = pd.DataFrame(cosmos_ids)
    cosmos_ids_df['id'] = cosmos_ids_df['id'].apply(str)
    cosmos_ids = cosmos_ids_df['id']

    # ************************ prepare SQL statements *****************************************************

    # clear stage table
    if_exists = " if exists( select 1 from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '"+targetStageSchema+"' AND TABLE_NAME = '"+targetStageTable+"')"
    stg_truncate_query = '  truncate table '+targetStageSchema +'.'+ targetStageTable
    stg_truncate_query = if_exists + stg_truncate_query

    # clear table that tracks the deleted records
    if_del_exists = " if exists( select 1 from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '"+targetStageSchema+"' AND TABLE_NAME = '"+targetDeleteTable+"')"
    del_query = ' truncate table '+targetStageSchema +'.'+ targetDeleteTable
    del_query = if_del_exists + del_query

    #truncate the stage table and the table to track the deletes
    tn = engine.execute(stg_truncate_query)
    dc = engine.execute(del_query)

    # get the ids from the existing data in SQL table.
    # This will be compared with azure data to identify the records that are to be deleted from SQL table.

    # define query to get the ids from the target SQL table
    id_query = 'select id from  '+ targetSchema+'.'+ targetTable

    # define the connection string to the target database
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server="+targetServer+";"
                          "Database="+targetDB+";"
                          "Trusted_Connection=yes;")

    # get Ids from SQL db
    db_ids = get_id_from_sql_database(id_query,cnxn)
    db_ids['id'] =  db_ids['id'].apply(str)

    # find the ids that are in SQL but not in cosmos
    diff = db_ids[~db_ids.id.isin(cosmos_ids)]
    msg = str(len(diff)) + ' item(s) will be deleted....'
    print (msg)

    #*********************** Download data *************************************************
    # execute the batch file that was created earlier
    project_root = os.path.abspath(os.path.dirname(__file__))
    definition_root = os.path.join(project_root, 'azure_data',batfile)
    process=subprocess.Popen([definition_root],cwd='azure_data') # cwd: sets the current working directory
    process.wait()
    # the above commands download the data from azure and save them in 'azure_data' directory as a json file
    # it waits till the download is complete.

    #*********************** Consume downloaded data *************************************************
    output_loc = 'azure_data/'+output_file_name
    # read the json file into a DataFrame to get a tabular structure
    imp = pd.read_json(output_loc)

    # if there are rows to be deleted, send their ids to the target table to track the deletes.
    if (len(diff) > 0):
        diff.to_sql(targetDeleteTable, engine,schema=targetStageSchema, if_exists='append',chunksize=10000)

    # send the new and changed records to the stage table
    imp.to_sql(targetStageTable, engine,schema=targetStageSchema, if_exists='append',chunksize=10000)

    # define query to get the max time stamp after the stage table is refreshed.
    tsquery = 'select max(_ts) as mT from ' + targetStageSchema+'.'+ targetStageTable
    result = engine.execute(tsquery)

    # return the max time stamp -> this will be stored in SubscriptionDownloadConfig table for subsequent incremental downloads.
    # max time stamp is derived from the '_ts' field.
    # This is an autogenerated field in all Cosmos documents that gives the UNIX timestamp when the document was created/updated.

    maxTimeStampImport = 0
    for items in result:
        maxTimeStampImport = items[0]
    return (maxTimeStampImport)


def get_max_timestamp_from_sql_database(subscriptionId,cnxn):
    data_source_query = "exec usp_get_last_timeStamp " + "'"+subscriptionId+"'"
    config_data = pd.read_sql(data_source_query, cnxn).to_dict('index')
    return config_data[0]['LastTimeStamp']


def set_sql_subscription_config_data(subscriptionId,maxTimeStamp,cnxn):
    data_source_query = "exec usp_update_SubscriptionDownloadConfig " + "'"+subscriptionId+"',"+ str(maxTimeStamp)
    config_data = pd.read_sql(data_source_query, cnxn)
    return config_data
