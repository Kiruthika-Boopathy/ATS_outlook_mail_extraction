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
    unread_mails = []
    client_name = []

    mail_url = f"https://graph.microsoft.com/v1.0//me/mailFolders?$filter=displayName eq '{client}'&$expand=childFolders, messages($filter=isRead eq false)"
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
    global conn
    try:
        message_id = email_data['message_id']
        conn = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        cursor = conn.cursor()

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
                                    job_description,
                                    job_status,
                                    client_id,
                                    business_unit,
                                    job_bill_rate,
                                    job_created_at,
                                    job_created_by,
                                    no_of_positions,
                                    job_id
                                ) VALUES (
                                    %(job_start_date)s,
                                    %(job_end_date)s,
                                    %(client_jobid)s,
                                    %(location)s,
                                    %(job_title)s,
                                    %(job_description)s,
                                    %(job_status)s,
                                    %(client)s,
                                    %(business_unit)s,
                                    %(job_bill_rate)s,
                                    %(job_created_at)s,
                                    'System',
                                    %(no_of_positions)s,
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
                    "business_unit": result_data['business_unit'],
                    "job_bill_rate": result_data['job_bill_rate'],
                    "job_created_at": result_data['job_created_at'],
                    "no_of_positions": result_data['no_of_positions'],
                    "job_id": str(uuid.uuid4())
                }
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute(insert_query, data_to_insert)
                mark_unread_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
                requests.patch(mark_unread_url, json={"isRead": True}, headers=headers)
        conn.commit()
        return "successfully inserted"
    except mysql.connector.Error as error:
        print(f"Error inserting data into MySQL: {error}")


def update_into_mysql(formatted_results, headers):
    global conn, cursor
    try:
        conn = mysql.connector.connect(host=db_config['host'],
                                       user=db_config['user'],
                                       password=db_config['password'],
                                       database=db_config['database'])
        cursor = conn.cursor()
        for result_data in formatted_results:
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
                update_query = """UPDATE jobpostings SET job_status = %(job_status)s,comments = %(comments)s
                                    WHERE client_jobid = %(client_jobid)s"""
                cursor.execute(update_query, data_to_update)
                mark_unread_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
                requests.patch(mark_unread_url, json={"isRead": True}, headers=headers)
            else:
                mark_read_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
                requests.patch(mark_read_url, json={"isRead": False}, headers=headers)
        conn.commit()
        return "successfully updated"
    except mysql.connector.Error as error:
        print(f"Error updating data into MySQL: {error}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def alabama_update(lines, headers):
    global formatted_result, formatted_results, job_bill_rate

    date_received = datetime.strptime(lines['date_received'], "%Y-%m-%dT%H:%M:%SZ")
    formatted_date_received = date_received.strftime("%Y-%m-%d %H:%M:%S")
    subject = lines['subject']
    message_id = lines['message_id']
    email_body = lines['body'].replace("#", "")
    lines = email_body.split('\n')

    for i, line in enumerate(lines):
        if "Rate:" in line:
            job_bill_rate = line.split("$")[1].strip()

    client_jobid = formatted_reason = formatted_reasons = None

    for line in lines:
        line = line.strip()
        if "Requisition : " in line:
            client_jobid = line.split("Requisition : ")[1].strip().split()[0]
        elif "Requisition:" in line:
            client_jobid = line.split("Requisition:")[1].strip().split()[0]
        if "closed" in line:
            formatted_reason = line.split("closed")[1].strip()
        elif "Decline Reason :" in line:
            formatted_reasons = line.split("Decline Reason :")[1].strip()

    formatted_results = []
    if "has been closed" in subject:
        formatted_result = {
            "client_jobid": client_jobid,
            "job_status": "closed",
            "reason": formatted_reason,
            "date_received": formatted_date_received,
            "message_id": message_id
        }
    elif "Declined" in subject:
        formatted_result = {
            "client_jobid": client_jobid,
            "job_status": "Declined",
            "reason": formatted_reasons,
            "job_bill_rate": job_bill_rate,
            "date_received": formatted_date_received,
            "message_id": message_id
        }
    formatted_results.append(formatted_result)
    result = update_into_mysql(formatted_results, headers)
    return result


def alabama_insert(lines, VMS_data):
    formatted_results = []

    job_start_date = job_end_date = location = job_title = client = client_jobid = no_of_positions = job_bill_rate = job_status = None
    job_description = ""

    date_received = datetime.strptime(lines['date_received'], "%Y-%m-%dT%H:%M:%SZ")
    formatted_date_received = date_received.strftime("%Y-%m-%d %H:%M:%S")
    email_body = lines['body'].split('\n')

    for i, line in enumerate(email_body):
        if "Requisition #:" in line:
            client_jobid = line.split(":")[1].strip()
        elif line.strip().startswith("Job Category:"):
            job_title = line.split(":")[1].strip()
        elif line.strip().startswith("Contractor's Work Location:"):
            location = line.split(":")[1].strip()

    capturing_description = False
    for line in email_body:
        if line.strip().startswith("Broadcast Comments :") or line.strip().startswith(
                "Position Description :"):
            field_name, value = line.split(":", 1)
            field_name = field_name.strip()
            value = value.strip().replace(' ', '').replace('|', '')
            if field_name == "Position Description":
                capturing_description = True
            job_description += f"{field_name}: {value}\n"
            if capturing_description and field_name != "Position Description":
                capturing_description = False
        elif capturing_description:
            if "You may click on the following link" in line:
                break
            job_description += line + "\n"

    formatted_result_data = {
        "job_start_date": job_start_date,
        "job_end_date": job_end_date,
        "location": location,
        "job_title": job_title,
        "client": VMS_data[0][0],
        "client_jobid": client_jobid,
        "business_unit": None,
        "no_of_positions": int(no_of_positions) if no_of_positions else None,
        "job_status": "Pending",
        "job_bill_rate": job_bill_rate,
        "job_description": job_description,
        "job_created_at": formatted_date_received,
        "job_created_by": "System",
        "job_id": str(uuid.uuid4())
    }
    formatted_results.append(formatted_result_data)
    return formatted_results


def maricopa_insert(VMS_data, unread_mails):
    extracted_list = []
    for email_data in unread_mails:
        email_body = email_data['body']
        email_subject = email_data['subject']
        date_received = datetime.strptime(email_data['date_received'], "%Y-%m-%dT%H:%M:%SZ")
        formatted_date_received = date_received.strftime("%Y-%m-%d %H:%M:%S")
        if 'Open Position Notification' in email_subject:
            client_jobid = None
            if "Requisition #:" in email_body:
                requisition_index = email_body.find("Requisition #:") + len("Requisition #:")
                end_index = email_body.find("\n", requisition_index)
                if end_index != -1:
                    client_jobid = email_body[requisition_index:end_index].strip()

            job_title = None
            if "Job Category:" in email_body:
                category_index = email_body.find("Job Category:") + len("Job Category:")
                end_index = email_body.find("\n", category_index)
                if end_index != -1:
                    job_title = email_body[category_index:end_index].strip()

            location = None
            if "Contractor's Work Location:" in email_body:
                location_index = email_body.find("Contractor's Work Location:") + len("Contractor's Work Location:")
                end_index = email_body.find("\n", location_index)
                if end_index != -1:
                    location = email_body[location_index:end_index].strip()
            formatted_start_date = None
            if 'Elections team through' in email_body:
                job_start_date_str = email_body.split('Elections team through', 1)[1].split('and possibly extend', 1)[
                    0].strip()
                job_start_date = datetime.strptime(job_start_date_str, '%m/%d/%y')
                formatted_start_date = job_start_date.strftime('%Y-%m-%d %H:%M:%S') if job_start_date else None

            formatted_end_date = None
            if 'possibly extend through' in email_body:
                job_end_date_str = email_body.split('possibly extend through', 1)[1].split('\n', 1)[
                    0].strip().rstrip('.')
                try:
                    job_end_date = datetime.strptime(job_end_date_str, '%m/%d/%Y')
                    formatted_end_date = job_end_date.strftime('%Y-%m-%d %H:%M:%S') if job_end_date else None
                except ValueError:
                    print("Error parsing end date:", job_end_date_str)

            job_status = None
            if ' Open Position' in email_subject:
                job_status = 'open'

            job_description = None
            if "Position Description :" in email_body:
                job_description = email_body.split("Position Description :", 1)[1].replace('\n', '').split(
                    'PLEASE DO NOT REPLY TO THIS MESSAGE', 1)[0].strip('.')

            result_data_item = {
                "job_start_date": formatted_start_date,
                "job_end_date": formatted_end_date,
                "location": location,
                "job_title": job_title,
                "client": VMS_data[0][0],
                "client_jobid": client_jobid,
                "no_of_positions": 0,
                "job_status": job_status,
                "job_bill_rate": 0,
                "business_unit": None,
                "job_description": job_description,
                "job_created_at": formatted_date_received,
                "job_created_by": "System",
                "job_id": str(uuid.uuid4())
            }
            extracted_list.append(result_data_item)
            return extracted_list


def maricopa_update(unread_mails, headers):
    extracted_list = []
    result_data_item = {}
    for email_data in unread_mails:
        email_body = email_data['body']
        email_subject = email_data['subject']
        message_id = email_data['message_id']

        extracted_data = {
            'client_jobid': None,
            'job_title': None,
            'location': None,
            'job_description': None,
            'job_status': 'open',
            'comments': None,
        }
        if 'closed' in email_subject:
            lines = email_body.split('\n')
            for i, line in enumerate(lines):
                if line.startswith("Requisition#"):
                    extracted_data['client_jobid'] = line.split(":")[1].strip()
                elif line.startswith("Job Category"):
                    extracted_data['job_title'] = line.split(":")[1].strip()
                elif line.startswith("Location"):
                    extracted_data['location'] = line.split(":")[1].strip()

                elif 'closed' in email_data['subject']:
                    extracted_data['job_status'] = 'closed'
                    extracted_data['comments'] = ' the requisition has been closed due to all positions being filled.'

                result_data_item = {
                    'client_jobid': extracted_data['client_jobid'],
                    'job_title': extracted_data['job_title'],
                    'location': extracted_data['location'],
                    'reason': extracted_data['comments'],
                    'job_description': extracted_data['job_description'],
                    'job_status': extracted_data['job_status'],
                    "message_id": message_id
                }
    extracted_list.append(result_data_item)
    result = update_into_mysql(extracted_list, headers)
    return result


def main_acro_extraction():
    client_names = ["State_of_Alabama", "Maricopa_Country"]
    result = ''

    for client_name in client_names:
        headers = get_access_token(client_name)
        unread_mails, client = get_unread_emails(headers, client_name)
        VMS_data = extract_client_details(client)

        if unread_mails:
            for lines in unread_mails:
                if client[0] == "State_of_Alabama":
                    if "Open Position Notification" in lines['subject']:
                        formatted_results = alabama_insert(lines, VMS_data)
                        result = insert_into_mysql(formatted_results, lines, headers)

                    elif "Declined" in lines['subject'] or "has been closed" in lines['subject']:
                        result = alabama_update(lines, headers)

                if client[0] == "Maricopa_Country":
                    if "Open Position Notification" in lines['subject']:
                        formatted_results = maricopa_insert(VMS_data, unread_mails)
                        result = insert_into_mysql(formatted_results, lines, headers)

                    elif "has been closed" in lines['subject'] or "Declined" in lines['subject']:
                        result = maricopa_update(unread_mails, headers)
    return result


main_acro_extraction()
