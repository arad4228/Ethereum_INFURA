from web3 import Web3
import pandas as pd
from secrete import *

w3 = Web3(Web3.HTTPProvider(INFURA_URL))

# Check if an address is EOA
def is_eoa(address):
    checksum_address = w3.to_checksum_address(address)
    return w3.eth.get_code(checksum_address) == b''

chunk_df = pd.read_csv("chunk_3.csv")
#chunk_df = pd.read_csv("C:\\Users\\PET\\Desktop\\논문작성관련\\self-trade\\230914\\result_1.csv")
# Drop rows with NaN or empty values in 'from_address' or 'to_address'
chunk_df = chunk_df.dropna(subset=['from_address', 'to_address'])

# Convert the addresses to checksum format after handling NaN values
chunk_df['from_address'] = chunk_df['from_address'].apply(w3.to_checksum_address)
chunk_df['to_address'] = chunk_df['to_address'].apply(w3.to_checksum_address)

eoa_df = chunk_df[
    (chunk_df['from_address'].apply(is_eoa)) & 
    (chunk_df['to_address'].apply(is_eoa))
]

# Save the filtered dataframe to result_1.csv
eoa_df.to_csv("reulst_4.csv", index=False)