import csv
import random
import string
import os

# Function to create a random string
def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# Function to generate a random number
def random_int():
    return random.randint(1000, 9999)

# Create a CSV file approximately 100MB
def generate_large_csv(file_name, row_count):
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(["id", "name", "age", "email", "address"])
        
        for i in range(row_count):
            # Write data to CSV file
            writer.writerow([
                i,  # ID
                random_string(10),  # name
                random_int(),  # age
                random_string(5) + "@example.com",  # email
                random_string(20)  # address
            ])
    
            # Print progress every 1 million rows
            if i % 1000000 == 0 and i > 0:
                print(f"{i} rows written...")
    
    print(f"File {file_name} generated successfully!")

# Calculate the number of rows to create a CSV file of approximately 100 MB
# Assume each row has an average of about 100-150 bytes (depending on the specific data size).
# Currently the file is 41.89 MB, need to increase by about 2.5 times
row_size = 150  # bytes per row, average estimate
target_file_size = 100 * 1024 * 1024  # 100 MB
current_file_size = 41.89 * 1024 * 1024  # 41.89 MB

# Increase the number of rows by 2.5 times
estimated_row_count = int((target_file_size / current_file_size) * (target_file_size // row_size))

# Create CSV file
file_name = "large_file_100MB.csv"
generate_large_csv(file_name, estimated_row_count)

# Check file size
file_size = os.path.getsize(file_name)
print(f"Generated file size: {file_size / (1024 * 1024):.2f} MB")
