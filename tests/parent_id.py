import pandas as pd

def fixed_dictionary():

    path = r'C:\GIS\Projektai\Judumas\data_processing\JudumasFunctions\20230722_0.csv'

    df = pd.read_csv(path)
    df['parent_global_id'] = df['GlobalID']
    df['parent_global_id'] = df.apply(lambda row: f"{{{row['parent_global_id']}}}", axis=1)
    print(df)

if __name__ == '__main__':
    fixed_dictionary()