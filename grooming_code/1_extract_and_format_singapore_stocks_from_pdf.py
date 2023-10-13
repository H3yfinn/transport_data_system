#%%
import pytesseract
from PIL import Image
import csv
import pandas as pd
import re
os.chdir(re.split('transport_data_system', os.getcwd())[0]+'/transport_data_system')

# Set the tesseract path in the script
pytesseract.pytesseract.tesseract_cmd = r"C:\Users\finbar.maunsell\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"#since im not meant to have this installed on work comp i deleted it, so you would need ot reinstall it if you want to run this code.

def image_to_csv(image_path: str, csv_output_path: str):
    """
    Converts an image containing tabular data to a CSV file using Tesseract for OCR.
    
    Parameters:
    image_path (str): The file path of the image to convert.
    csv_output_path (str): The file path where the CSV output should be saved.
    """
    # Open the image
    img = Image.open(image_path)
    
    # Use Tesseract to extract text
    text = pytesseract.image_to_string(img)
    
    # Split the text into lines
    lines = text.split("\n")
    
    # Filter out empty lines
    lines = [line for line in lines if line.strip()]
    
    # Initialize an empty dictionary to hold the processed data
    processed_data = {}
    
    # Loop through each line in the OCR output
    for line in lines:
        # Split the row by the pipe character if it exists; otherwise split by spaces
        split_char = '|' if '|' in line else '  '
        
        # Split line and clean up the leading and trailing whitespace
        cleaned_line = [cell.strip() for cell in line.split(split_char) if cell.strip()]
        
        # Separate the header and the data
        header = cleaned_line[0]
        values = cleaned_line[1:]
        
        # Add to the processed_data dictionary
        processed_data[header] = values
    
    # Find the maximum length among all lists in processed_data
    max_length = max(len(values) for values in processed_data.values())

    # Pad shorter lists with None
    for key, values in processed_data.items():
        while len(values) < max_length:
            values.append(None)

    # Now, create the DataFrame
    df = pd.DataFrame(processed_data)
    
    # Save the DataFrame to a CSV file
    df.to_csv(csv_output_path, index=False)

    return df.head()
# Example usage (you'll have to provide actual paths)
image_to_csv('input_data/Singapore/MVP01-1_MVP_by_type.png', 'input_data/Singapore/vehicle_stocks_by_type.csv')

#%%