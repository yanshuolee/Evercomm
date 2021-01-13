import smtplib
gmail_user = 'plee@evercomm.com'
gmail_password = 'plee99'
from_add = gmail_user
to = ["plee@evercomm.com"]
cc = ["hlee@evercomm.com", "dwyang@evercomm.com"]
subject = 'testing'
body = 'Hello World! \nThis is a test'
email_text = """\
From: %s
To: %s
CC: %s
Subject: %s
%s
"""%(from_add, ", ".join(to), ", ".join(cc), subject, body)

smtpObj = smtplib.SMTP('evercomm.com', 587)
smtpObj.ehlo()
smtpObj.starttls()
smtpObj.login(gmail_user, gmail_password)
smtpObj.sendmail(from_add, to+cc, email_text)
smtpObj.close()
print ('Email sent')
