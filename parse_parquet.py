import pandas as pd

def extract_domains_from_parquet(file_path = "logos.snappy.parquet"):
    df = pd.read_parquet(file_path)
    company_list = df["domain"].dropna().tolist()
    return company_list

#print(extract_domains_from_parquet())

#aeccglobal.my
#flashbay.es
#linde.ch