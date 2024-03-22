import os
import pandas as pd

def read_text_files_from_folder(folder_path):
    data = []
    for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as file:
                file_content = file.read()
                # data.append({'Filename': filename, 'Content': file_content})
                data.append(file_content)
    return data

def write_to_excel(data, excel_file):
    df = pd.DataFrame(data)
    df.to_excel(excel_file, index=False)

if __name__ == "__main__":
    folder_path = '/Users/trtr/Desktop/Jan31'
    excel_file = '/Users/trtr/Desktop/Jan31.xlsx'

    text_data = read_text_files_from_folder(folder_path)
    write_to_excel(text_data, excel_file)