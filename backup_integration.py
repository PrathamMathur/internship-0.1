import fitz  
import psycopg2
from psycopg2 import sql

def extract_text_from_pdf(file_path):
    pdf_document = fitz.open(file_path)
    extracted_text = ""
    
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)
        extracted_text += page.get_text()
    
    pdf_document.close()
    return extracted_text

def insert_text_into_postgresql(texts):
    try:
        conn = psycopg2.connect(
            database="Control Table",
            user="postgres",
            password="1234",
            host="localhost",
            port="5432"
        )

        with conn:
            with conn.cursor() as cursor:
                create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS pdf_texts (id SERIAL PRIMARY KEY, text TEXT)")
                cursor.execute(create_table_query)
                
                

                insert_query = sql.SQL("INSERT INTO pdf_texts (text) VALUES (%s)")
                cursor.executemany(insert_query, [(text,) for text in texts])
                
                
                
                

    except psycopg2.Error as e:
        print("Error:", e)
    finally:
        if conn is not None:
            conn.close()

# Call the function with the file path
pdf_file_path = "C:/Users/mathu/OneDrive/Desktop/Lease-20230809T051356Z-001/Lease/Lease-PDF/19815_24-03-2022_LEASE.pdf"
extracted_text = extract_text_from_pdf(pdf_file_path)

# Split the extracted text into manageable chunks (e.g., paragraphs)
#text_chunks = extracted_text.split("\n\n")  # Modify this based on your PDF's structure
text_chunks = extracted_text.split("  ")  # Split by two or more spaces


# Insert the extracted text chunks into the PostgreSQL database
insert_text_into_postgresql(text_chunks)
print(extracted_text)
print("Text extracted and inserted into the PostgreSQL database.")
