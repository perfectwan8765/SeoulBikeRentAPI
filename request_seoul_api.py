import requests
import pandas as pd
import logging as log
import re
import datetime
import boto3
from botocore.exceptions import ClientError
import schedule
import time

g_type = 'json'
g_service = 'bikeList'
g_api_host = 'http://openapi.seoul.go.kr:8088'
g_collection_date = datetime.datetime.now()

def make_seoul_rent_bike_info_csv () :
    log.basicConfig(level=log.ERROR)

    global g_collection_date
    g_collection_date = datetime.datetime.now()
    station_csv_df = update_seoul_rent_bike_station_csv()

    start_index = 1
    end_index = 1
    
    while (end_index < station_csv_df.size) :
        start_index = end_index + 1
        end_index = start_index + 999

        response = request_seoul_api(start_index, end_index)
        result = find_code_from_json(response)

        if 'INFO-000' != result['CODE'] :
            code = result['CODE']
            message = result['MESSAGE']
            # log.error(f'ErrorCode: {code}, Message: {message}')
        else :
            make_csv_for_result_of_api(station_csv_df, response)

def update_seoul_rent_bike_station_csv() :
    # 서울 공공자전거 대여소 정보(21년 12월 기준)
    # 대여소별 상세정보 가져오기
    df = pd.read_csv('seoul_rent_bike_station_info_21y_12m_after.csv', skiprows=[1,2,3,4], encoding='utf-8')
    # 대여소 정보 csv 파일 정제
    # key = '대여소\r\n번호'
    # Dataframe Address Column rename
    df = df.rename(columns = {'대여소\r\n번호': 'stationNum', '소재지(위치)': 'gu', 'Unnamed: 3' : 'address'} , inplace = False)

    return df

def find_code_from_json (dic_str) :
    search_list = [dic_str]

    for search in search_list :
        for key in search.keys() :
            if key == 'CODE' :
                return search
            elif type(search[key]) is dict :
                search_list.append(search[key])

def request_seoul_api(start_index, end_index) :
    with open('api_key.bin', encoding='UTF-8') as api_key_file :
        api_key = api_key_file.read()
    url = f'{g_api_host}/{api_key}/{g_type}/{g_service}/{start_index}/{end_index}/'

    return requests.get(url).json()

def make_csv_for_result_of_api(station_csv_df, response) :
    global g_collection_date
    result_json = response.get('rentBikeStatus').get('row')
    no_address_list = []

    for station in result_json :
        update_station_dict(station)
        row = station_csv_df.loc[station_csv_df['stationNum'] == station['stationNum']] # type DataFrame

        if not row.empty :
            station['address'] = re.sub('\r\n', ' ', row.address.item()) # 상세주소
            station['gu'] = row.gu.item() # 구
        else :
            # logging.error('No Address of station : ' + station['stationName'])
            no_address_list.append(station['stationName'])

    result_df = pd.DataFrame.from_dict(result_json)
    result_df = result_df[
        ['stationId', 'stationNum', 'stationName', 'gu', 'address', 'stationLatitude', 'stationLongitude', 'rackTotCnt', 'parkingBikeTotCnt', 'shared', 'collectionDate']
    ]

    file_name = 'seoul_rentbike_info_{}.csv'.format(g_collection_date.strftime('%m_%d_%Y'))

    result_df.to_csv(file_name, encoding='UTF-8', index=False)

    if len(no_address_list) != 0 :
        with open('no_address_list.txt', 'w', encoding='UTF-8') as outfile:
            for station in no_address_list :
                outfile.write(station + '\n')

    # upload_file(file_name, 'seoulrentbike')

def update_station_dict (dict) :
    global g_collection_date
    station_name = dict['stationName']
    station_name_split = station_name.strip('"').split('.')

    dict['stationName'] = f'{station_name_split[0]}.{station_name_split[1].lstrip()}'
    dict['stationNum'] = int(station_name_split[0])
    dict['collectionDate'] = g_collection_date.strftime('%m/%d/%Y')

def upload_file(file_name, bucket, object_name=None):
    global g_collection_date
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = '{}/{}'.format(g_collection_date.strftime('%Y/%m'), file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        log.error(e)


# schedule.every().day.at('08:25').do(make_seoul_rent_bike_info_csv)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

make_seoul_rent_bike_info_csv()