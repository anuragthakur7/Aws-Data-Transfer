import glob
import argparse
import io
import json
import math
import os
import sys
import time
from configparser import ConfigParser
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import boto3
import paramiko
from botocore.config import Config
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.creation_information import FileCreationInformation
from office365.sharepoint.files.file import File

from RevoLogger.logger import Logger
from common.utils import get_s3_client, get_s3_session
from common.constant import LoadType


def download_and_upload_on_s3(source_client, destination_client, source_path, dest_path, src_bucket, trgt_bucket,
                              file_nm):
    print("in download and upload on s3 function")
    file = os.path.join(source_path, file_nm)
    target_location = os.path.join(dest_path, file_nm)
    print("source_client", source_client)
    print("destination client", destination_client)
    print("source file :", file)
    print("target : ", target_location)
    try:
        source_response = source_client.get_object(Bucket=src_bucket, Key=file)
        print("source_response", source_response)

        destination_client.upload_fileobj(source_response['Body'], trgt_bucket, target_location)
    except Exception as e:
        print(e)
        return False
    return True


def s3_connection(conf):
    if conf['iam_role'] == '':
        print("get s3 session")
        s3_resource = get_s3_client(conf['access_key'], conf['secret_key'])
        print(s3_resource)
    else:
        print("get s3 session using iam role")
        s3_resource = get_s3_resource(conf)
    return s3_resource


def s3_conn(conf):
    if conf['iam_role'] == '':
        print("get s3 session")
        s3_resource = get_s3_session(conf['access_key'], conf['secret_key'])
        print(s3_resource)
    else:
        print("get s3 session using iam role")
        s3_resource = s3_get_resource(conf)
    return s3_resource


def open_ftp_connection(conf):
    """
    Opens ftp connection and returns connection object
    """
    ftp_host = conf['host']
    ftp_port = int(conf['port'])
    ftp_username = conf['username']
    ftp_password = conf['password']
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    try:
        transport = paramiko.Transport(ftp_host, ftp_port)
    except Exception as e:
        return "conn_error"
    try:
        transport.connect(username=ftp_username, password=ftp_password)
    except Exception as identifier:
        return "auth_error"
    ftp_connection = paramiko.SFTPClient.from_transport(transport)
    return ftp_connection


def transfer_chunk_from_ftp_to_s3(ftp_file, s3, multipart_upload, bucket_name, ftp_file_path, s3_file_path, part_number, chunk_size,):
    start_time = time.time()
    chunk = ftp_file.read(int(chunk_size))
    part = s3.upload_part(
        Bucket=bucket_name,
        Key=s3_file_path,
        PartNumber=part_number,
        UploadId=multipart_upload["UploadId"],
        Body=chunk,
    )
    end_time = time.time()
    total_seconds = end_time - start_time
    print(
        "speed is {} kb/s total seconds taken {}".format(
            math.ceil((int(chunk_size) / 1024) / total_seconds), total_seconds
        )
    )
    part_output = {"PartNumber": part_number, "ETag": part["ETag"]}
    return part_output


def get_bucket(conf):
    return conf['bucket_name']


def get_s3_resource(s3_conf):
    try:
        if s3_conf['region'] != '':
            my_config = Config(
                region_name=s3_conf['region'],
                signature_version='v4',
                retries={
                    'max_attempts': 10,
                    'mode': 'standard'
                }
            )
            sts_client = boto3.client('sts', config=my_config)
        else:
            sts_client = boto3.client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn=s3_conf['iam_role'],
            RoleSessionName='s3_transfer'
        )
    except Exception as e:
        print("Unable to assume role. Please check role_arn : ", e)
        logger.error('Unable to assume role. Please check role_arn')
        raise
    credentials = assumed_role_object['Credentials']
    return boto3.client('s3',
                        aws_access_key_id=credentials['AccessKeyId'],
                        aws_secret_access_key=credentials['SecretAccessKey'],
                        aws_session_token=credentials['SessionToken'],
                        )


def s3_get_resource(s3_conf):
    try:
        if s3_conf['region'] != '':
            my_config = Config(
                region_name=s3_conf['region'],
                signature_version='v4',
                retries={
                    'max_attempts': 10,
                    'mode': 'standard'
                }
            )
            sts_client = boto3.client('sts', config=my_config)
        else:
            sts_client = boto3.client('sts')
        assumed_role_object = sts_client.assume_role(
            RoleArn=s3_conf['iam_role'],
            RoleSessionName='s3_transfer'
        )
    except Exception as e:
        print("Unable to assume role. Please check role_arn : ", e)
        logger.error('Unable to assume role. Please check role_arn')
        raise
    credentials = assumed_role_object['Credentials']
    return boto3.resource('s3',
                          aws_access_key_id=credentials['AccessKeyId'],
                          aws_secret_access_key=credentials['SecretAccessKey'],
                          aws_session_token=credentials['SessionToken'],
                          )


def get_sftp_connection(conf):
    logger.info("Checking sftp connection.")
    print("Checking sftp connection.")
    con = paramiko.SSHClient()
    con.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    con.connect(conf['host'], int(conf['port']), conf['username'], conf['password'])
    # print(con)

    return con


def sftp_file_check(arg_f_lst, arg_prf):
    file_to_process = []
    if len(arg_f_lst) > 0:
        if arg_prf is not None:
            for file1 in arg_f_lst:
                if file1.startswith(arg_prf):
                    file_to_process.append(file1)

        else:
            raise Exception("Prefix or File is mandatory")
    if len(file_to_process) == 0:
        return False, []
    return True, file_to_process


def s3_to_s3_transfer(logger, conf):
    s3_src_conf = config[conf['src_placeholder']]
    source_client = s3_connection(s3_src_conf)
    s3_dest_conf = config[conf['dest_placeholder']]
    destination_client = s3_connection(s3_dest_conf)
    source_bucket = s3_src_conf["bucket_name"]
    s3 = s3_conn(s3_src_conf)
    source_bucket_obj = s3.Bucket(source_bucket)
    dest_bucket = s3_dest_conf["bucket_name"]
    print("s3 connections created ")
    if conf['prefix'] != '':
        prefix = conf['prefix']
        logger.info(f"Checking file on {prefix} location")
        print(f"Checking file on {prefix} location")
        for s3_object in source_bucket_obj.objects.filter(Prefix=prefix):
            s3_file = s3_object.key
            file_name = s3_file.split('/')[-1]
            download_and_upload_on_s3(source_client, destination_client, conf['source_path'],
                                      conf['dest_path'], source_bucket, dest_bucket,
                                      file_name)

    elif conf['file_list']:
        logger.info("file transfer started.")
        print("file transfer started.")
        for file_name in conf['file_list']:
            print("file name : ", file_name)
            download_and_upload_on_s3(source_client, destination_client, conf['source_path'],
                                      conf['dest_path'], source_bucket, dest_bucket,
                                      file_name)

    else:
        logger.exception("Invalid Type")
        print("Invalid Type")


def transfer_file_from_ftp_to_s3(bucket_name, ftp_file_path, s3_file_path, conf):
    ftp_connection = open_ftp_connection(conf)
    chunk_size = conf['chunk_size']
    ftp_file = ftp_connection.file(ftp_file_path, "r")
    s3_connection = boto3.client("s3")
    ftp_file_size = ftp_file._get_size()
    try:
        s3_file = s3_connection.head_object(Bucket=bucket_name, Key=s3_file_path)
        if s3_file["ContentLength"] == ftp_file_size:
            print("File Already Exists in S3 bucket")
            ftp_file.close()
            return
    except Exception as e:
        pass
    if ftp_file_size <= int(chunk_size):
        # upload file in one go
        print("Transferring complete File from FTP to S3...")
        ftp_file_data = ftp_file.read()
        ftp_file_data_bytes = io.BytesIO(ftp_file_data)
        s3_connection.upload_fileobj(ftp_file_data_bytes, bucket_name, s3_file_path)
        print("Successfully Transferred file from FTP to S3!")
        ftp_file.close()

    else:
        print("Transferring File from FTP to S3 in chunks...")
        # upload file in chunks
        chunk_count = int(math.ceil(ftp_file_size / float(chunk_size)))
        multipart_upload = s3_connection.create_multipart_upload(
            Bucket=bucket_name, Key=s3_file_path
        )
        parts = []
        for i in range(chunk_count):
            print("Transferring chunk {}...".format(i + 1))
            part = transfer_chunk_from_ftp_to_s3(
                ftp_file,
                s3_connection,
                multipart_upload,
                bucket_name,
                ftp_file_path,
                s3_file_path,
                i + 1,
                chunk_size,
            )
            parts.append(part)
            print("Chunk {} Transferred Successfully!".format(i + 1))

        part_info = {"Parts": parts}
        s3_connection.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_file_path,
            UploadId=multipart_upload["UploadId"],
            MultipartUpload=part_info,
        )
        print("All chunks Transferred to S3 bucket! File Transfer successful!")
        ftp_file.close()


def sftp_to_s3(logger, conf):
    sftp_con = get_sftp_connection(config[conf['src_placeholder']])
    client = sftp_con.open_sftp()
    s3_dest_conf = config[conf['dest_placeholder']]
    dest_bucket = get_bucket(s3_dest_conf)
    if conf['prefix'] != '':
        c_prefix = conf['prefix']
        logger.info(f"Checking file on {c_prefix} location")
        print(f"Checking file on {c_prefix} location")
        file_list = client.listdir(conf['source_path'])
        file_check, files_to_process = sftp_file_check(
            file_list, c_prefix
        )
        for file_name in files_to_process:
            src_file_nm = os.path.join(conf['source_path'], file_name)
            trgt_file_nm = os.path.join(conf['dest_path'], file_name)
            ftp_connection = open_ftp_connection(config[conf['src_placeholder']])
            if ftp_connection == "conn_error":
                print("Failed to connect FTP Server!")
            elif ftp_connection == "auth_error":
                print("Incorrect username or password!")
            else:
                try:
                    ftp_file = ftp_connection.file(src_file_nm, "r")
                except Exception as e:
                    print("File does not exists on FTP Server!")
                transfer_file_from_ftp_to_s3(dest_bucket, src_file_nm, trgt_file_nm,
                                             config[conf['src_placeholder']])

    elif conf['file_list']:
        logger.info("file transfer started.")
        print("file transfer started.")
        for file_name in conf['file_list']:
            src_file_nm = os.path.join(conf['source_path'], file_name)
            trgt_file_nm = os.path.join(conf['dest_path'], file_name)
            ftp_connection = open_ftp_connection(config[conf['src_placeholder']])
            if ftp_connection == "conn_error":
                print("Failed to connect FTP Server!")
            elif ftp_connection == "auth_error":
                print("Incorrect username or password!")
            else:
                try:
                    ftp_file = ftp_connection.file(src_file_nm, "r")
                except Exception as e:
                    print("File does not exists on FTP Server!")
                transfer_file_from_ftp_to_s3(dest_bucket, src_file_nm, trgt_file_nm,
                                             config[conf['src_placeholder']])
    else:
        logger.exception("Invalid Type")
        print("Invalid Type")


def transfer_file_from_s3_to_sftp(src_bucket, source_client, source_path, dest_path, ftp_connection,
                                  file_nm):
    print("in transfer file from s3 to sftp function")
    file = os.path.join(source_path, file_nm)
    target_location = os.path.join(dest_path, file_nm)
    print("source_client", source_client)
    print("source file :", file)
    print("target : ", target_location)
    try:
        source_response = source_client.get_object(Bucket=src_bucket, Key=file)
        print("source_response", source_response)

        ftp_connection.putfo(source_response['Body'], target_location)
    except Exception as e:
        print(e)
        return False
    return True


def s3_to_sftp(logger, conf):
    sftp_con = get_sftp_connection(config[conf['dest_placeholder']])
    client = sftp_con.open_sftp()
    s3_src_conf = config[conf['src_placeholder']]
    source_client = s3_connection(s3_src_conf)
    src_bucket = get_bucket(s3_src_conf)
    s3 = s3_conn(s3_src_conf)
    source_bucket_obj = s3.Bucket(src_bucket)
    if conf['prefix'] != '':
        prefix = conf['prefix']
        logger.info(f"Checking file on {prefix} location")
        print(f"Checking file on {prefix} location")
        try:
            for s3_object in source_bucket_obj.objects.filter(Prefix=prefix):
                src_key = s3_object.key
                file_name = src_key.split('/')[-1]
                transfer_file_from_s3_to_sftp(src_bucket, source_client, conf['source_path'],
                                              conf['dest_path'], client, file_name)
        except Exception as ex:
            print(ex)

    elif conf['file_list']:
        logger.info("file transfer started.")
        print("file transfer started.")
        for file_name in conf['file_list']:
            transfer_file_from_s3_to_sftp(src_bucket, source_client, conf['source_path'], conf['dest_path'],
                                          client, file_name
                                          )

    else:
        logger.exception("Invalid Type")
        print("Invalid Type")


def s3_to_sp_prefix(logger, s3_con, bucket_name, prefix, client_id, client_secret, url,
                    dest_path):
    aws_bucket = s3_con.Bucket(bucket_name)
    logger.info(f"Checking file on {prefix} location")
    print(f"Checking file on {prefix} location")
    outputDir = "/mnt/Additional_Backup/temp_dir/"
    for src_key in aws_bucket.objects.filter(Prefix=prefix):
        s3_file = src_key.key
        file = s3_file.split('/')[-1]
        destination = os.path.join(outputDir, file)
        print(destination)
        aws_bucket.download_file(s3_file, destination)
    sp_upload(client_id, client_secret, url, dest_path)
    for file_name in os.listdir(outputDir):
        file = os.path.join(outputDir, file_name)
        if os.path.isfile(file):
            os.remove(file)


def s3_to_sp_file_list(logger, s3_con, bucket_name, file_list, client_id, client_secret, url, source_path,
                       dest_path):
    aws_bucket = s3_con.Bucket(bucket_name)
    outputDir = "/mnt/Additional_Backup/temp_dir/"
    logger.info("file transfer started.")
    print("file transfer started.")
    for file_nm in file_list:
        s3_file = os.path.join(source_path, file_nm)
        destination = os.path.join(outputDir, file_nm)
        print(destination)
        aws_bucket.download_file(s3_file, destination)
    sp_upload(client_id, client_secret, url, dest_path)
    for file_name in os.listdir(outputDir):
        file = os.path.join(outputDir, file_name)
        if os.path.isfile(file):
            os.remove(file)


def sp_upload(client_id, client_secret, url,
              dest_path):
    client_id = client_id
    client_secret = client_secret
    url = url

    ctx_auth = AuthenticationContext(url)
    if ctx_auth.acquire_token_for_app(client_id, client_secret):
        ctx = ClientContext(url, ctx_auth)
        path_files = "/mnt/Additional_Backup/temp_dir/*.*"

        for path in glob.glob(path_files):
            with open(path, 'rb') as content_file:
                file_content = content_file.read()

            # dest_folder="Shared Documents/TEST/"
            target_folder = ctx.web.get_folder_by_server_relative_url(dest_path)
            name = os.path.basename(path)

            info = FileCreationInformation(url=name, content=file_content, overwrite=True)

            target_file = target_folder.files.add(info)
            ctx.execute_query()
            print("File successfully uploaded !")

    else:
        print(ctx_auth.get_last_error())


def sp_to_s3_prefix(logger, s3, trgt_bucket, prefix, client_id, client_secret, url,
                    dest_path):
    logger.info(f"Checking file on {prefix} location")
    print(f"Checking file on {prefix} location")
    url = url
    client_id = client_id
    client_secret = client_secret

    context_auth = AuthenticationContext(url)
    context_auth.acquire_token_for_app(client_id=client_id, client_secret=client_secret)

    ctx = ClientContext(url, context_auth)

    relativeUrl = prefix
    libraryRoot = ctx.web.get_folder_by_server_relative_url(relativeUrl)
    ctx.load(libraryRoot)
    ctx.execute_query()
    files = libraryRoot.files
    ctx.load(files)
    ctx.execute_query()

    outputDir = "/mnt/Additional_Backup/temp_dir/"
    print(outputDir)
    for myfile in files:
        pathList = myfile.properties["ServerRelativeUrl"].split('/')
        fileDest = outputDir + "/" + pathList[-1]
        response = File.open_binary(ctx, myfile.properties['ServerRelativeUrl'])
        with open(fileDest, "wb") as local_file:
            local_file.write(response.content)
        try:
            target_location = os.path.join(dest_path, pathList[-1])
            with open(fileDest, "rb") as f:
                s3.upload_fileobj(f, trgt_bucket, target_location)
        except Exception as exp:
            print(str(exp))

    for file_name in os.listdir(outputDir):
        file = os.path.join(outputDir, file_name)
        if os.path.isfile(file):
            os.remove(file)


def sp_to_s3_file_list(logger, s3, bucket_name, file_list, client_id, client_secret, url, source_path,
                       dest_path):
    url = url
    client_id = client_id
    client_secret = client_secret

    context_auth = AuthenticationContext(url)
    context_auth.acquire_token_for_app(client_id=client_id, client_secret=client_secret)

    ctx = ClientContext(url, context_auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    outputDir = "/mnt/Additional_Backup/temp_dir/"
    logger.info("file transfer started.")
    print("file transfer started.")
    for file_nm in file_list:
        src_file = os.path.join(source_path, file_nm)
        trgt_file = os.path.join(outputDir, file_nm)
        response = File.open_binary(ctx, src_file)
        with open(trgt_file, "wb") as local_file:
            local_file.write(response.content)
        try:
            target_location = os.path.join(dest_path, file_nm)
            with open(trgt_file, "rb") as f:
                s3.upload_fileobj(f, bucket_name, target_location)
        except Exception as e:
            print(str(e))
    for file_name in os.listdir(outputDir):
        file = os.path.join(outputDir, file_name)
        if os.path.isfile(file):
            os.remove(file)


def sharepoint_to_s3(logger, conf):
    sp_conf = config[conf['src_placeholder']]
    s3_conf = config[conf['dest_placeholder']]
    client_id = sp_conf["client_id"]
    client_secret = sp_conf["client_secret"]
    url = sp_conf["url"]
    s3 = s3_connection(s3_conf)
    bucket_name = s3_conf["bucket_name"]
    prefix = conf['prefix']

    file_list = conf['file_list']
    dest_path = conf['dest_path']
    if prefix != '':
        sp_to_s3_prefix(logger, s3, bucket_name, prefix, client_id, client_secret, url,
                        dest_path)
    elif file_list:
        sp_to_s3_file_list(logger, s3, bucket_name, file_list, client_id, client_secret, url, conf['source_path'],
                           dest_path)
    else:
        print("invalid type")


def s3_to_sharepoint(logger, conf):
    s3_conf = config[conf['src_placeholder']]
    sp_conf = config[conf['dest_placeholder']]
    client_id = sp_conf["client_id"]
    client_secret = sp_conf["client_secret"]
    url = sp_conf["url"]
    bucket_name = s3_conf["bucket_name"]
    prefix = conf['prefix']
    s3 = s3_conn(s3_conf)
    file_list = conf['file_list']
    dest_path = conf['dest_path']
    if prefix != '':
        s3_to_sp_prefix(logger, s3, bucket_name, prefix, client_id, client_secret, url,
                        dest_path)
    elif file_list:

        s3_to_sp_file_list(logger, s3, bucket_name, file_list, client_id, client_secret, url, conf['source_path'],
                           dest_path)
    else:
        print("invalid type")


def process_request(logger, json_config):
    try:

        main_confs = json_config['main_conf']
        for main_conf in main_confs:
            if main_conf.get('active', True):
                if int(main_conf['load_type']) == LoadType.s3_to_s3.value:
                    print("s3 to s3 script execution started")
                    logger.info("s3 to s3 script execution started")
                    s3_to_s3_transfer(logger, main_conf)
                elif int(main_conf['load_type']) == LoadType.sftp_to_s3.value:
                    print("sftp to s3 script execution started")
                    logger.info("sftp to s3 script execution started")
                    sftp_to_s3(logger, main_conf)
                elif int(main_conf['load_type']) == LoadType.s3_to_sftp.value:
                    print("s3 to sftp script execution started")
                    logger.info("s3 to sftp script execution started")
                    s3_to_sftp(logger, main_conf)
                elif int(main_conf['load_type']) == LoadType.sharepoint_to_s3.value:
                    print("sharepoint to s3 script execution started")
                    logger.info("sharepoint to s3 script execution started")
                    sharepoint_to_s3(logger, main_conf)
                elif int(main_conf['load_type']) == LoadType.s3_to_sharepoint.value:
                    print("s3 to sharepoint script execution started")
                    logger.info("s3 to sharepoint script execution started")
                    s3_to_sharepoint(logger, main_conf)
                else:
                    print("invalid type")
    except Exception as exc:
        logger.exception(str(exc))


if __name__ == "__main__":
    run_path = sys.argv[0]
    if not os.path.isabs(run_path):
        dir_path = os.getcwd()
    else:
        dir_path = os.path.dirname(run_path)
    print(dir_path)
    with open(os.path.join(dir_path, "common", "logger.json"), 'r') as log_file:
        log_json = log_file.read()
    dict_log = json.loads(log_json)
    dict_log['handler_list'][0]['filename'] = os.path.join(dir_path, dict_log['handler_list'][0]['filename'])
    logger = Logger(config_json=dict_log)
    print("COE Scripts execution started")
    author = 'Vishwajeet'
    version = 'v1.0'
    print('Author : ', author, '\n')
    print('Version : ', version, '\n')
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--config', type=str, required=True)
        args = parser.parse_args()
        config_path = args.config
        config = ConfigParser(interpolation=None)
        config.read(os.path.join(dir_path, "common", "settings.ini"))
        logger.info(f"file path : {config_path}")
        print(f"------Running for File Path : {config_path}--------")

        with open(config_path, 'r') as config_file:
            data = config_file.read()

        json_config = json.loads(data)
        process_request(logger, json_config)
        print(f"==== S3 Transfer Script Successfully Executed ====")
    except Exception as e:
        print("*** S3 Transfer Script Execution Failed. Please check logs ***")
        logger.exception(str(e))
