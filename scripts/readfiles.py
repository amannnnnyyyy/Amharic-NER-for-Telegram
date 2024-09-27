import pandas as pd
def read_conll(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = [line.strip().split() for line in f if line.strip()]
    return pd.DataFrame(data)