from datetime import datetime
import time
import uuid
import html2text
import mysql.connector
import base64
import json
import requests

client_id = "961c0447-457b-4cfb-ba65-984565d405c8"
client_secret = "dtc8Q~gnOSvbeNmAp41Rm7uVifX0sXwsvBqriduM"

db_config = {
    'host': 'vivyahire-dev.cdk5imbtnucf.ap-south-1.rds.amazonaws.com',
    'user': 'admin',
    'password': 'dR5Y81zZI5wo3p3X',
    'database': 'vivyahire_dev'
}


def get_access_token(client_name):
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )
    cursor = conn.cursor()

    token_data = f"SELECT api_key FROM clients WHERE client_name = '{client_name}'"
    cursor.execute(token_data)
    existing_client = cursor.fetchone()
    token_data = existing_client[0]
    byte_code = bytes(token_data, 'utf-8')
    decoded_token = base64.b64decode(byte_code.decode('utf-8'))
    token_data = json.loads(decoded_token)
    if is_token_valid(token_data):
        access_token = token_data['token_datas']
    else:
        access_token = refresh_access_token(token_data['refresh_token'], client_name)
    headers = {
        'Authorization': f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    return headers


def is_token_valid(token_data):
    current_time = time.time()
    expiry_date_iso = token_data.get('expiry_date', '1970-01-01T00:00:00Z')
    expiry_date = datetime.strptime(expiry_date_iso, '%Y-%m-%dT%H:%M:%S.%fZ')
    expiry_timestamp = expiry_date.timestamp()
    return current_time < expiry_timestamp


def refresh_access_token(refresh_token, client_name):
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )
    cursor = conn.cursor()
    token_url = 'https://login.microsoftonline.com/common/oauth2/v2.0/token'
    token_data = {
        'client_id': client_id,
        'scope': 'https://graph.microsoft.com/.default',
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token',
        'client_secret': client_secret
    }

    response = requests.post(token_url, data=token_data)

    if response.status_code == 200:
        token_json = response.json()
        if 'access_token' in token_json:
            token_expiration = token_json['expires_in']
            expiration_timestamp = time.time() + token_expiration
            expiration_datetime = datetime.fromtimestamp(expiration_timestamp)
            expiration_formatted = expiration_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            token_data = {
                'token_datas': token_json['access_token'],
                'refresh_token': refresh_token,
                'expires_in': token_json['expires_in'],
                'expiry_date': expiration_formatted
            }
            token_value = str(token_data).replace("'", "\"")
            token_value = base64.b64encode(token_value.encode('utf-8'))
            json_token = str(token_value).removesuffix("'").removeprefix("b'")
            update_access_key = f"UPDATE clients SET api_key = '{json_token}' WHERE client_name = '{client_name}'"
            cursor.execute(update_access_key)
            conn.commit()
            return token_data['token_datas']
        return token_json['token_datas']
    else:
        raise Exception(f"Token Refresh Error: {response.status_code} - {response.text}")


def get_unread_emails(headers, client):
    client_name = []
    unread_mails = []

    mail_url = (f"https://graph.microsoft.com/v1.0//me/mailFolders?$filter=displayName eq '{client}'&$expand"
                f"=childFolders, messages($filter=isRead eq false)")
    response = requests.get(mail_url, headers=headers)

    if response.status_code == 200:
        mails = response.json().get("value", [])
        for mail in mails:
            if mail['messages'] and mail['displayName']:
                client_name.append(mail['displayName'])
                for message in mail['messages']:
                    html_body = message['body']['content']
                    plain_text_body = html2text.html2text(html_body)
                    email_data = {
                        "subject": message["subject"],
                        "sender": message['from']['emailAddress']['address'],
                        "date_received": message['receivedDateTime'],
                        "body": plain_text_body,
                        "message_id": message['id']
                    }
                    unread_mails.append(email_data)

    else:
        print(f"Error: {response.status_code} - {response.text}")
    return unread_mails, client_name


def extract_client_details(client_name):
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )
    result = []

    try:
        with conn.cursor() as cursor:
            query = f"SELECT client_id FROM clients WHERE client_name = '{client_name[0]}'"
            cursor.execute(query)

            result.append(cursor.fetchone())

        if result:
            return result
        else:
            return None
    except mysql.connector.Error as error:
        print(f"Error connecting to MySQL: {error}")
    finally:
        if conn.is_connected():
            conn.close()


def insert_into_mysql(formatted_results, email_data, headers):
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )
    cursor = conn.cursor()
    try:
        message_id = email_data['message_id']
        for result_data in formatted_results:
            cursor.execute(
                "SELECT COUNT(*) FROM jobpostings WHERE client_jobid = %s",
                (result_data['client_jobid'],)
            )
            record_count = cursor.fetchone()[0]

            if record_count == 0:
                insert_query = """
                       INSERT INTO jobpostings (
                           job_start_date,
                           job_end_date,
                           client_jobid,
                           location,
                           job_title,
                           job_bill_rate,
                           no_of_positions,
                           job_description,
                           job_status,
                           client_id,
                           job_created_at,
                           job_created_by,
                           job_id
                       ) VALUES (
                           %(job_start_date)s,
                           %(job_end_date)s,
                           %(client_jobid)s,
                           %(location)s,
                           %(job_title)s,
                           %(job_bill_rate)s,
                           %(no_of_positions)s,
                           %(job_description)s,
                           %(job_status)s,
                           %(client)s,
                           %(job_created_at)s,
                           'System',
                           %(job_id)s
                       )
                   """

                data_to_insert = {
                    "job_start_date": result_data['job_start_date'],
                    "job_end_date": result_data['job_end_date'],
                    "client_jobid": result_data['client_jobid'],
                    "location": result_data['location'],
                    "job_title": result_data['job_title'],
                    "job_description": result_data['job_description'],
                    "job_status": result_data['job_status'],
                    "client": result_data['client'],
                    "job_bill_rate": result_data['job_bill_rate'],
                    "no_of_positions": result_data['no_of_positions'],
                    "job_created_at": result_data['job_created_at'],
                    "job_id": str(uuid.uuid4())
                }
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute(insert_query, data_to_insert)
                mark_unread_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
                requests.patch(mark_unread_url, json={"isRead": True}, headers=headers)

        conn.commit()
        return cursor.fetchall()
    except mysql.connector.Error as error:
        print(f"Error inserting data into MySQL: {error}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def update_into_mysql(result_datas, headers):
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )
    cursor = conn.cursor()
    try:
        for result_data in result_datas:
            message_id = result_data['message_id']
            select_query = "SELECT client_jobid FROM jobpostings WHERE client_jobid = %s"
            cursor.execute(select_query, (result_data['client_jobid'],))
            existing_client = cursor.fetchone()
            data_to_update = {
                "job_status": result_data["job_status"],
                "client_jobid": result_data['client_jobid'],
                "comments": result_data['reason']
            }

            if existing_client:
                # Update the existing record
                update_query = """UPDATE jobpostings SET job_status = %(job_status)s, comments = %(comments)s 
                WHERE client_jobid = %(client_jobid)s"""
                cursor.execute(update_query, data_to_update)
                mark_unread_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
                requests.patch(mark_unread_url, json={"isRead": True}, headers=headers)
            else:
                mark_read_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
                requests.patch(mark_read_url, json={"isRead": False}, headers=headers)


        conn.commit()
        return cursor.fetchall()
    except mysql.connector.Error as error:
        print(f"Error inserting data into MySQL: {error}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def acceleration_update(lines, headers):
    formatted_results = []

    client_jobid = None
    date_received = datetime.strptime(lines['date_received'], "%Y-%m-%dT%H:%M:%SZ")
    formatted_date_received = date_received.strftime("%Y-%m-%d %H:%M:%S")
    subject = lines['subject']
    message_id = lines['message_id']
    email_body = lines['body'].replace("#", "")

    client_jobid = email_body.split('Job Requisition', 1)[1].split('has', 1)[0].rstrip(' ').strip()
    formatted_result = ""
    reason = ""
    reason1 = ""
    if "Job Filled Notification" in subject:
        reason1 = email_body.split('has', 1)[1].strip().split('\n  \n\n **Job Information**', 1)[0].rstrip(' ')
    elif "Job Requisition On-Hold" in subject:
        reason = email_body.split('has temporarily', 1)[1].strip().split('\n  \n\n **Job Information**', 1)[0].rstrip(
            ' ')

    if "Job Filled Notification" in subject:
        formatted_result = {
            "client_jobid": client_jobid,
            "job_status": "closed",
            "reason": reason1,
            "date_received": formatted_date_received,
            "message_id": message_id
        }
    elif "Job Requisition On-Hold" in subject:
        formatted_result = {
            "client_jobid": client_jobid,
            "job_status": "hold",
            "reason": reason,
            "date_received": formatted_date_received,
            "message_id": message_id
        }
    formatted_results.append(formatted_result)
    result = update_into_mysql(formatted_results, headers)
    return result


def acceleration_mail_extract_information(unread_emails, VMS_data):
    global job_description
    formatted_results = []
    job_start_date = job_end_date = location = job_title = description = status = client = client_jobid = \
        no_of_positions = job_bill_rate = None

    for email_data in unread_emails:
        email_body = email_data['body']
        date_received = datetime.strptime(email_data['date_received'], "%Y-%m-%dT%H:%M:%SZ")
        formatted_date_received = date_received.strftime("%Y-%m-%d %H:%M:%S")

        if "Job Requisition #" in email_body:
            client_jobid = email_body.split('Job Requisition #', 1)[1].split('has', 1)[0].rstrip(' ')

        if "Job Location:\n\n|\n\n" in email_body:
            location = email_body.split("Job Location:\n\n|\n\n", 1)[1].strip().split('\n  \nJob Title:', 1)[0].strip()
            location = location.replace(' ', '').replace('|', '')

        if 'Job Title:\n\n|\n\n' in email_body:
            job_title = email_body.split("Job Title:\n\n|\n\n", 1)[1].strip().split(' \n  \nJob Type:', 1)[0].strip()
            job_title = job_title.replace(' ', '').replace('|', '')

        if 'Number of Contingent Workers Required:\n\n|\n\n' in email_body:
            no_of_positions = email_body.split("Number of Contingent Workers Required:\n\n|\n\n", 1)[1].strip().split(
                ' \n  \nStart Date:', 1)[0].strip()
            no_of_positions = no_of_positions.replace(' ', '').replace('|', '')

        if 'Start Date:\n\n|\n\n' in email_body:
            job_start_date = email_body.split("Start Date:\n\n|\n\n", 1)[1].strip().split('\n  \nEnd Date:', 1)[
                0].strip()
            job_start_date = job_start_date.replace(' ', '').replace('|', '')
            parsed_date = datetime.strptime(job_start_date, '%m/%d/%Y')
            job_start_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')

        if 'End Date:\n\n|\n\n' in email_body:
            job_end_date = \
            email_body.split("End Date:\n\n|\n\n", 1)[1].strip().split("\n  \nReport To Manager's Office Address:", 1)[
                0].strip()
            job_end_date = job_end_date.replace(' ', '').replace('|', '')
            parsed_date = datetime.strptime(job_end_date, '%m/%d/%Y')
            job_end_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')

        if 'Job Requisition #' in email_body:
            job_description = \
            email_body.split("Job Requisition #", 1)[1].strip().split("  \n  \nFor additional inquiries", 1)[0].strip(
                ' ')
            job_description = job_description.replace(' ', '').replace('|', '')

        formatted_result = {
            "job_start_date": job_start_date,
            "job_end_date": job_end_date,
            'job_bill_rate': job_bill_rate,
            "client_jobid": client_jobid,
            "location": location,
            "job_title": job_title,
            "job_description": job_description,
            "job_status": 'pending',
            "client": VMS_data[0][0],
            "business_unit": None,
            "job_created_by": "System",
            "job_created_at": formatted_date_received,
            "no_of_positions": no_of_positions,
        }

        formatted_results.append(formatted_result)
        return formatted_results


def main_vms_extraction():
    client_names = ["Chromalloy"]
    result = ''

    for client_name in client_names:
        headers = get_access_token(client_name)
        unread_emails, client = get_unread_emails(headers, client_name)
        VMS_data = extract_client_details(client)

        if unread_emails:
            for lines in unread_emails:
                if client[0] == "Chromalloy":
                    if "New Requisition" in lines['subject']:
                        formatted_results = acceleration_mail_extract_information(unread_emails, VMS_data)
                        result = insert_into_mysql(formatted_results, lines, headers)

                    elif "Job Filled Notification" in lines['subject']:
                        result = acceleration_update(lines, headers)

                    elif "Job Requisition On-Hold" in lines['subject']:
                        result = acceleration_update(lines, headers)

    return result


main_vms_extraction()
