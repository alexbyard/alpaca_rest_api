import pandas as pd 
import numpy as np

def stack_data(df_dict: dict):
    
    df_list = [df for _, df in df_dict.items()]

    stack_df = pd.concat(df_list)

    return stack_df