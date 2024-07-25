import pandas as pd

def normalise_data(df):
    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}{sep}{i}", item))
            else:
                items.append((new_key, v))
        return dict(items)

    # Apply flattening to each row
    flattened_data = df.apply(lambda row: flatten_dict(row.to_dict()), axis=1)
    
    # Create a new DataFrame from the flattened data
    flattened_df = pd.DataFrame(flattened_data.tolist())
    
    # Replace all NaN values with "No Data"
    flattened_df = flattened_df.fillna("No Data")
    
    # Convert all columns to string type
    flattened_df = flattened_df.astype(str)
    
    return flattened_df