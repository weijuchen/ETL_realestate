def _crawl_and_download_zip(year:int, season:int,ti=None):
    import requests
    # import os
    REAL_ESTATE_URL=f"https://plvr.land.moi.gov.tw//DownloadSeason?season={year}S{season}&type=zip&fileName=lvr_landcsv.zip"
    # api="https://plvr.land.moi.gov.tw//DownloadSeason?season="+ str(year)+ "S"+ str(season)+ "&type=zip&fileName=lvr_landcsv.zip"
    response=requests.get(REAL_ESTATE_URL)
    if response.status_code !=200:
        print("request failed")
        raise AirflowFailException
    else:
        zip_file_path=f"/tmp/{year}{season}.zip"
        # path=f"/usr/local/airflow/include/{year}{season}.zip"
        # path='user/local/airflow/include/'+str(year)+str(season)+".zip"
        with open(zip_file_path,"wb") as f:
            f.write(response.content)
        output_data={
            "response_path":zip_file_path,
            "year":year,
            "season":season
        }    
        ti.xcom_push(key="zip_file_info",value=output_data)
        # ti.xcom_push(key='response_path',value=zip_file_path)    
        print("here is the path",zip_file_path)
        # return path  





def _extract_zip_file(ti=None):
    import os
    import zipfile
    zip_file_info = ti.xcom_pull(key='zip_file_info',task_ids="crawl_and_download_zip")  # 得到完整字典
    path=zip_file_info['response_path']
    year=zip_file_info['year']  
    season=zip_file_info['season']
    print(f"here is path {path} here is the year{year} season {season}")
    # folder=f"{path}/realEstate{year}{season}"
    folder=f"real_estate{year}{season}"
    # file_name=f"{year}{season}.zip"


    # make additional folder for files to extract    
    if not os.path.isdir(folder):
        os.mkdir(folder)


    # extract files to the folder
    with zipfile.ZipFile(path,"r") as zip_ref:
    # with zipfile.ZipFile(file_name,"r") as zip_ref:
        zip_ref.extractall(folder)
    ti.xcom_push(key='folder',value=folder) 
    print(f"folder: {folder}  path : {path}")    


def _store_prices(stock):
    client = _get_minio_client()
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'

