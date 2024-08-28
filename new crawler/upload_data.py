# save the data - s3

import os
from pathlib import Path
import datetime
from dotenv import load_dotenv
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime

# e.g.
# ecommerce : naver
# crawling_type : model, checkout...
# category : car..
# ctg_fullname

# s3 저장 경로
# ecommerce : naver  -> 전체 경로 : naver-detailpage
# crawling_type : image, reivew, meta
# category : travel-adapter
# ctg_fullname : product_name + item_name -> str(df.columns[0]) + '_' +  str(df.columns[1])

###=================save the reivew=====================
def save_review_data(data, ecommerce, crawling_type, category, search_type, file_type, keyword, file_format='parquet', bucket_name='ailee-crawling-data'): 
    now = datetime.now()
    # file_name = f'{product_name}_{now.strftime("%Y%m%d")}.{file_format}'
    file_name = f'{keyword}_{now.strftime("%Y%m%d-%H:%M:%S")}.{file_format}'
    s3_folder_path = f'{ecommerce}-detailpage/{crawling_type}/{category}/{search_type}/{file_type}/{keyword}/'
    s3_key = s3_folder_path + file_name

    # Initialize S3 clientrmse
    dotenv_path = Path("C:/Users/tips_workstation2/Desktop/new_crawler/.env") # your path
    load_dotenv(dotenv_path=dotenv_path)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY_ID")
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY_ID)

    # Convert DataFrame to Parquet format in memory
    if file_format == 'parquet':
        buffer = BytesIO()
        table = pa.Table.from_pandas(data)
        pq.write_table(table, buffer)
        buffer.seek(0)
    elif file_format == 'csv':
        buffer = BytesIO()
        data.to_csv(buffer, index=False)
        buffer.seek(0)
    else:
        raise ValueError("Unsupported file format. Please use 'parquet' or 'csv'.")

    # Upload the data to S3
    try:
        s3.upload_fileobj(buffer, bucket_name, s3_key)
        print(f"file successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file to s3: {e}")

#=======save the review image=========
def save_review_image_data(ecommerce, crawling_type, category, search_type, file_type, jpg_file_path, jpg_fullname, keyword, file_format='jpg', bucket_name='ailee-crawling-data'):
    now = datetime.now()
    file_name = f'{jpg_fullname}_{now.strftime("%Y%m%d-%H:%M:%S")}.{file_format}'
    s3_folder_path = f'{ecommerce}-detailpage/{crawling_type}/{category}/{search_type}/{file_type}/{keyword}/'
    s3_key = s3_folder_path + file_name

    # Initialize S3 client
    dotenv_path = Path("C:/Users/tips_workstation2/Desktop/new_crawler/.env") # your path
    load_dotenv(dotenv_path=dotenv_path)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY_ID")
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

    # Upload the data to S3
    try:
        s3.upload_file(jpg_file_path, bucket_name, s3_key)
        print(f"jpg successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading PDF to S3: {e}")


###==========save the image pdf==================
def save_image_pdf_data(ecommerce, crawling_type, category, search_type, file_type, pdf_file_path, ctg_fullname, file_format='jpg', bucket_name='ailee-crawling-data'):
    now = datetime.now()
    file_name = f'{ctg_fullname}_{now.strftime("%Y%m%d-%H:%M:%S")}.{file_format}'
    s3_folder_path = f'{ecommerce}-detailpage/{crawling_type}/{category}/{search_type}/{file_type}/'
    s3_key = s3_folder_path + file_name

    # Initialize S3 client
    dotenv_path = Path("C:/Users/tips_workstation2/Desktop/new_crawler/.env") # your path
    load_dotenv(dotenv_path=dotenv_path)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY_ID")
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

    # Upload the data to S3
    try:
        s3.upload_file(pdf_file_path, bucket_name, s3_key)
        print(f"jpg successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading PDF to S3: {e}")


###============save the image====================
def save_image_jpg_data(ecommerce, crawling_type, category, file_type, jpg_file_path, jpg_fullname, file_format='jpg', bucket_name='ailee-crawling-data'):
    now = datetime.now()
    file_name = f'{jpg_fullname}_{now.strftime("%Y%m%d-%H:%M:%S")}.{file_format}'
    s3_folder_path = f'{ecommerce}-detailpage/{crawling_type}/{category}/{file_type}/'
    s3_key = s3_folder_path + file_name

    # Initialize S3 client
    dotenv_path = Path("C:/Users/tips_workstation2/Desktop/new_crawler/.env") # your path
    load_dotenv(dotenv_path=dotenv_path)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY_ID")
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

    # Upload the data to S3
    try:
        s3.upload_file(jpg_file_path, bucket_name, s3_key)
        print(f"PDF successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading PDF to S3: {e}")



###==========save the meta data using query============
def save_meta_query_data(data, ecommerce, crawling_type, category, search_type, query, file_format='parquet', bucket_name='ailee-crawling-data'): 
    now = datetime.now()
    file_name = f'meta_{query}_{now.strftime("%Y%m%d-%H:%M:%S")}.{file_format}' # _{file_format}
    s3_folder_path = f'{ecommerce}-detailpage/{crawling_type}/{category}/{search_type}/'
    s3_key = s3_folder_path + file_name

    # Initialize S3 client
    dotenv_path = Path("C:/Users/tips_workstation2/Desktop/new_crawler/.env") 
    load_dotenv(dotenv_path=dotenv_path)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY_ID")
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY_ID)

    # Convert DataFrame to Parquet format in memory
    if file_format == 'parquet':
        buffer = BytesIO()
        table = pa.Table.from_pandas(data)
        pq.write_table(table, buffer)
        buffer.seek(0)
    elif file_format == 'csv':
        buffer = BytesIO()
        data.to_csv(buffer, index=False)
        buffer.seek(0)
    else:
        raise ValueError("Unsupported file format. Please use 'parquet' or 'csv'.")

    # Upload the data to S3
    try:
        s3.upload_fileobj(buffer, bucket_name, s3_key)
        print(f"file successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file to s3: {e}") 



#=========save the meta data using ctg4=============
def save_meta_ctg4_data(data, ecommerce, crawling_type, category, search_type, category4_id, file_format='parquet', bucket_name='ailee-crawling-data'): 
    now = datetime.now()
    file_name = f'meta_{category}_{category4_id}_{now.strftime("%Y%m%d-%H:%M:%S")}.{file_format}'
    s3_folder_path = f'{ecommerce}-detailpage/{crawling_type}/{category}/{search_type}/'
    s3_key = s3_folder_path + file_name

    # Initialize S3 client
    dotenv_path = Path("C:/Users/tips_workstation2/Desktop/new_crawler/.env") 
    load_dotenv(dotenv_path=dotenv_path)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY_ID")
    s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY_ID)

    # Convert DataFrame to Parquet format in memory
    if file_format == 'parquet':
        buffer = BytesIO()
        table = pa.Table.from_pandas(data)
        pq.write_table(table, buffer)
        buffer.seek(0)
    elif file_format == 'csv':
        buffer = BytesIO()
        data.to_csv(buffer, index=False)
        buffer.seek(0)
    else:
        raise ValueError("Unsupported file format. Please use 'parquet' or 'csv'.")

    # Upload the data to S3
    try:
        s3.upload_fileobj(buffer, bucket_name, s3_key)
        print(f"file successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file to s3: {e}")


