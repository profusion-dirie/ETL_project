
import smtplib
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from sqlalchemy.orm import declarative_base
import sqlalchemy as db
import pymysql
# establishing connection
user = "diriei"
password = "sMVwBBECgvY3Zc9L"

host = "mh0adb03.profusion.com"
port = "3306"
database = "_sandbox"
engine = db.create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user=user, password=password, host=host, port=int(port), database=database))
connection = engine.connect()
metadata = db.MetaData()

business_census = db.table('Business_Census', metadata, autoload=True, autoload_with=engine)

Borough_Mapping = db.Table('Borough_Mapping', metadata,
                           autoload=True, autoload_with=engine)
query = db.select([Borough_Mapping])
ResultProxy = connection.execute(query)
ResultSet = ResultProxy.fetchall()


df = pd.DataFrame(ResultSet)
df.columns = ResultSet[0].keys()
df['borough_name'] = df['borough_name'].replace('\r', '', regex=True)
print(df.head())

# Sending email with dataframe as HTML
# recipients = ['diriei@profusion.com']
# emaillist = [elem.strip().split(',') for elem in recipients]
# msg = MIMEMultipart()
# msg['Subject'] = "practice"
# msg['From'] = 'diriei@profusion.com'

# html = """\
# <html>
#   <head>Hello, please find this practice email.
#   <br> </br>
#   Thanks.
#   <br> </br>
#   </head>
#   <body>
#   <br> Here's the borough data:
#   <br>
#     {0} 
#   </body>
# </html>
# """.format(df.to_html())

# part1 = MIMEText(html, 'html')
# msg.attach(part1)

# server = smtplib.SMTP('localhost')
# server.sendmail(msg['From'], emaillist, msg.as_string())
# server.quit()
