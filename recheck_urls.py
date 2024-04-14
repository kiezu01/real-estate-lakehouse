import pandas as pd

# Load the data
df = pd.read_csv('crawler/data/urls_test.csv', header=None)

# Check for duplicates
duplicates = df[df.duplicated()]
print(f"Found {len(duplicates)} duplicates")

# Drop duplicates
def drop_duplicates(df):
    df.drop_duplicates(inplace=True)
    print("Duplicates dropped successfully")

# Save the file
def save_file(df):
    df.to_csv('crawler/data/urls_test.csv', header=False, index=False)
    print("File saved successfully")
    
# Call the function
drop_duplicates(df)
save_file(df)
