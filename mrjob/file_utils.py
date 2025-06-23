import pandas as pd
from unidecode import unidecode
import json

def convert_xlsx_to_csv(xlsx_path, csv_path):

    df = pd.read_excel(xlsx_path, engine='openpyxl')
    
    df.columns = [unidecode(str(col)) for col in df.columns]
    df = df.applymap(lambda x: unidecode(str(x)) if pd.notnull(x) else x)

    for col in df.columns:
        df[col] = df[col].map(lambda x: unidecode(str(x)) if pd.notnull(x) else x)
    df.to_csv(csv_path, index=False)


def save_to_file(results, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({", ".join(key) if isinstance(key, list) else key: value for key, value in results}, f, ensure_ascii=False, indent=2)