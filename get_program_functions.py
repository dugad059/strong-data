import boto3
from env import *
from email.message import EmailMessage
import ssl
import smtplib

def get_send_program(BUCKET_NAME, KEY_NAME):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    url = s3.generate_presigned_url(
    ClientMethod='get_object', 
    Params={'Bucket': BUCKET_NAME, 'Key': KEY_NAME},
    ExpiresIn=3600)

    email_sender = 'davidmdfitness@gmail.com'
    email_password = pword
    email_receiver = 'daviddugas1@gmail.com'


    subject = 'Your DMDfitness Program'
    body = f'Here is your new program! The link expires after 1 hour, so if the link does not work, go back to the website and submit! {url}' 

    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(body)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.sendmail(email_sender, email_receiver, em.as_string())


