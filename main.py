import imaplib
import email
import pandas as pd
import psycopg2
from io import StringIO
import schedule
import time

def download_and_upload():
    
    mail = imaplib.IMAP4_SSL('imap.cision.com')
    mail.login('oscar.saborio@cision.com', 'MyPasswordIsASectret!*')
    mail.select('inbox')

    # Search for emails with attachments
    result, data = mail.search(None, '(BMW)')
    email_ids = data[0].split()

    for email_id in email_ids:
        result, data = mail.fetch(email_id, '(RFC822)')
        raw_email = data[0][1]
        msg = email.message_from_bytes(raw_email)

        for part in msg.walk():
            if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                continue

            if part.get_filename().endswith('.csv'):
                
                csv_data = part.get_payload(decode=True)
                csv_str = csv_data.decode('utf-8')

                
                df = pd.read_csv(StringIO(csv_str))

                
                conn = psycopg2.connect(
                    dbname='BMW', 
                    user='admin', 
                    password='admon', 
                    host='host'
                )
                cur = conn.cursor()

                for row in df.itertuples(index=False, name=None):
                    cur.execute("INSERT INTO your_table (column1, column2, ...) VALUES (%s, %s, ...)", row)

                conn.commit()
                cur.close()
                conn.close()

def job():
    download_and_upload()

# Schedule!!!!!!
schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
