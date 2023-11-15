import subprocess
# https://chat.openai.com/share/ad2b86f6-92d3-45db-b818-efcc94e6c303

def convert_caj_to_pdf(caj_file_path, pdf_file_path):
    try:
        # Run the caj2pdf command to convert the CAJ file to PDF
        subprocess.run(["caj2pdf", caj_file_path, "-o", pdf_file_path], check=True)
        print(f"Arr! Successfully converted {caj_file_path} to {pdf_file_path}, ye scallywag!")
    except subprocess.CalledProcessError as e:
        print(f"Yarrr! There be an error: {e}")
    except FileNotFoundError:
        print("Arr, me hearty! Ye need to install caj2pdf before ye can run this code.")

# Use the function to convert a CAJ file to PDF
convert_caj_to_pdf("path/to/your/file.caj", "path/to/your/output/file.pdf")
