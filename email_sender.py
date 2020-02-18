# this code was copied from the address bellow, read there the info to run it:
# https://docs.aws.amazon.com/ses/latest/DeveloperGuide/send-using-sdk-python.html
import boto3
from botocore.exceptions import ClientError
import argparse
import pandas as pd

BASE_DIR = "/home/ubuntu/scraping/"
SCRAPING_STATUS_FN = 'scraping_status.csv'

scraping_status = pd.read_csv(BASE_DIR + SCRAPING_STATUS_FN)

parser = argparse.ArgumentParser(description="provide sender and receiver email addresser for this notification \
\n (both most have already been verified byAWS SES)")

parser.add_argument('--sender', nargs=1, required=True,
                    help='email address through which SES will send the notification')
parser.add_argument('--recipients', nargs='+', required=True, help='email address(es) receiving the notification')
args = parser.parse_args()

AWS_REGION = "eu-west-1"

# The subject line for the email.
SUBJECT = "Amazon SES Test (SDK for Python)"

# The email body for recipients with non-HTML email clients.
BODY_TEXT = ("Scrape finished\r\n"
             f"{scraping_status}"
             )

# The HTML body of the email.
BODY_HTML = f"""<html>
<head></head>
<body>
  <h1>Scrape finished</h1>
  <p>{scraping_status.to_html()}</p>
</body>
</html>
"""

# The character encoding for the email.
CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
client = boto3.client('ses', region_name=AWS_REGION)

# Try to send the email.
try:
    # Provide the contents of the email.
    response = client.send_email(
        Destination={
            'ToAddresses': args.recipients,
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': BODY_HTML,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': BODY_TEXT,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
            },
        },
        Source=args.sender[0],
    )
except ClientError as e:
    print(e.response['Error']['Message'])
else:
    print("Email sent! Message ID:"),
    print(response['MessageId'])

