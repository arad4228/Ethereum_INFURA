import datetime
import threading
import pandas as pd
from web3 import Web3
from pprint import pprint as pp
# 여러개의 CSV를 처리하기 위해서 사용
from multiprocessing import Process, Manager
# 한 CSV에서 탐색하기 위해서 사용
from threading import Thread, Lock
from secrete import *

base_URL = "https://mainnet.infura.io/v3/"
base_Input_PATH = "./input/"
base_Output_PATH = "./output/"

MAX_Chunk_Number = 362
MAX_Quiry = 100000
MAX_Thread_Quiry = 20000
MAX_Process_Quiry = 80000

def is_eoa(w3, address, shared_URL_Limit_List, lock):
    try:
        checksum_address = w3.to_checksum_address(address)
        uri = (w3.provider.endpoint_uri).replace(base_URL,"")

        with lock:
            shared_URL_Limit_List[uri] += 1
        
        return w3.eth.get_code(checksum_address) == b''
    except Exception as e:
        shared_URL_Limit_List[uri] = 100000
        raise Exception(f"{uri} key is expired")
        

def find_available_url(shared_URL_Limit_List, used_urls, lock):
    with lock:
        for url in INFURA_URL_List:
            if used_urls[url] == 0 and MAX_Quiry - shared_URL_Limit_List[url] >= MAX_Process_Quiry:
                used_urls[url] = 1
                return url

        raise Exception("All API Keys expired or reached limit")


def work_thread(ith, chunk_df, return_df, url_INFURA, lock):
    w3 = Web3(Web3.HTTPProvider(base_URL + url_INFURA))
    print(f"{threading.current_thread().name} Start")

    chunk_df = chunk_df.copy()
    chunk_df['from_address'] = chunk_df['from_address'].apply(w3.to_checksum_address)
    chunk_df['to_address'] = chunk_df['to_address'].apply(w3.to_checksum_address)

    try:
        eoa_df = chunk_df[
            (chunk_df['from_address'].apply(lambda x: is_eoa(w3, x, shared_URL_Limit_List, lock))) & 
            (chunk_df['to_address'].apply(lambda x: is_eoa(w3, x, shared_URL_Limit_List, lock)))
        ]
        with lock:
            used_urls[url_INFURA] = 0
        print(f"{threading.current_thread().name} Done")
        return_df.insert(ith, eoa_df)

    except Exception as e:
        raise Exception(e)


def refine_INFURA(nProcess, file_Name, shared_URL_Limit_List, used_urls, lock):
    file_Path = base_Input_PATH + file_Name
    chunk_df = pd.read_csv(file_Path)
    url_INFURA = find_available_url(shared_URL_Limit_List, used_urls, lock)
    print(f"Process{nProcess} start reading {file_Name}")
    print(f"Process{nProcess} Using API-Key:{url_INFURA}")

    # Drop rows with NaN or empty values in 'from_address' or 'to_address'
    chunk_df = chunk_df.dropna(subset=['from_address', 'to_address'])
    thread_List = []
    output_df_list = []
    output_df_list.insert
    addition_Count = 10000
    start = 0
    
    for nThread in range(4):
        end = start + addition_Count
        thread = Thread(target=work_thread, args=(nThread, chunk_df.loc[start:end], output_df_list, url_INFURA, lock))
        thread_List.append(thread)
        start += addition_Count
        thread.start()
    print(f"All Thread Started at {datetime.datetime.now()}")
    
    for thread in thread_List:
        thread.join()

    print("All Thread Complete")
    eoa_df = pd.concat(output_df_list)

    # Save the filtered dataframe to result_1.csv
    now = datetime.datetime.now().strftime("%Y.%m.%d")
    eoa_df.to_csv(f"{base_Output_PATH}reulst_{file_Name}({now}).csv", index=False)

    print(f"Process{nProcess} Done\nOutput: reulst_{file_Name}({now}).csv")
    pp(dict(shared_URL_Limit_List))
    

if __name__ == "__main__":
    with Manager() as manager:
        shared_URL_Limit_List = manager.dict({key: 0 for key in INFURA_URL_List})
        used_urls = manager.dict({key: 0 for key in INFURA_URL_List})
        lock = manager.Lock()

        for i in range(5, MAX_Chunk_Number, len(INFURA_URL_List)):
                process_List = []
                try:
                    # Infura URL 갯수 만큼 코어를 늘려 할당한다.
                    for nProcess in range(len(INFURA_URL_List)):
                        file_name = f"chunk_{i + nProcess}.csv"
                        process = Process(target=refine_INFURA, args=(nProcess + 1, file_name, shared_URL_Limit_List, used_urls,lock))
                        process_List.append(process)
                        process.start()

                    for process in process_List:
                        process.join()
                except Exception as e:
                    print(e)

