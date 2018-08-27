
import smtplib
import os
from email import encoders

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase


def create_server(host, port, uid, pwd):
    """
    create smtp server based on args provided
    :param host: email server host
    :param port: port int used
    :param uid: username of account
    :param pwd: password of account
    :return: server obj
    """
    s = smtplib.SMTP(host, port)
    s.starttls()
    s.login(
        uid,
        pwd
    )
    return s


def create_email(_from, _to, _subj, _body, files):
    """
    create email from input args
    :param _from: email sent on behalf of
    :param _to: receiving email
    :param _subj: subj string
    :param _body: body string
    :param files: list of file paths to attach
    :return: email obj
    """
    msg = MIMEMultipart()
    msg['From'] = _from
    msg['To'] = _to
    msg['Subject'] = _subj
    msg.attach(MIMEText(_body, 'plain'))

    if files:
        for file in files:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(open(file, 'rb').read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename=' + os.path.basename(file))
            msg.attach(part)

    return msg
