import re
import base64
import zipfile
import io
from datetime import datetime, timedelta
from config import authenticate_gmail, upload_order_and_metadata


def search_recent_tmart_emails(service):
    after_ts = int((datetime.utcnow() - timedelta(hours=1)).timestamp())
    query = f'after:{after_ts} (subject:"TMart Purchase Orders" OR from:sherif.hossam@talabat.com)'
    results = service.users().messages().list(userId='me', q=query).execute()
    return results.get('messages', [])


def extract_order_date_from_subject(subject):
    match = re.search(r"\[(\d{4}-\d{2}-\d{2})\]", subject)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    return None


def fetch_and_upload_tmart_orders():
    service = authenticate_gmail()
    messages = search_recent_tmart_emails(service)
    print(f"Found {len(messages)} matching emails")

    for idx, msg in enumerate(messages, 1):
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        headers = msg_data.get("payload", {}).get("headers", [])
        subject = next((h["value"] for h in headers if h["name"] == "Subject"), "")
        sender = next((h["value"] for h in headers if h["name"] == "From"), "")

        if not subject.lower().startswith("tmart purchase orders") and "sherif.hossam@talabat.com" not in sender.lower():
            continue

        order_date_obj = extract_order_date_from_subject(subject)
        if not order_date_obj:
            print(f"Skipping email {idx}: no valid date found in subject")
            continue

        order_date = order_date_obj.strftime("%Y-%m-%d")
        delivery_date = (order_date_obj + timedelta(days=2)).strftime("%Y-%m-%d")
        client = "Talabat"
        status = "Pending"

        print(f"\nProcessing email {idx}")
        print(f"  Subject: {subject}")
        print(f"  From: {sender}")
        print(f"  Order Date: {order_date}")
        print(f"  Delivery Date: {delivery_date}")

        parts = msg_data["payload"].get("parts", [])
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for part in parts:
                filename = part.get("filename")
                body = part.get("body", {})
                if filename and "attachmentId" in body:
                    if not any(filename.lower().endswith(ext) for ext in ['.pdf', '.xls', '.xlsx', '.csv', '.zip']):
                        continue
                    att_id = body["attachmentId"]
                    attachment = service.users().messages().attachments().get(
                        userId='me',
                        messageId=msg['id'],
                        id=att_id
                    ).execute()
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode("UTF-8"))
                    zipf.writestr(filename, file_data)

        zip_buffer.seek(0)
        zip_filename = f"tmart_email_{idx}.zip"

        try:
            upload_response = upload_order_and_metadata(
                file_bytes=zip_buffer.read(),
                filename=zip_filename,
                client=client,
                order_type="Purchase Order",
                order_date=order_date,
                delivery_date=delivery_date,
                status=status,
                city=None,
                po_number=None
            )
            print(f"  Uploaded successfully. Supabase ID: {upload_response[0].get('id')}")
        except Exception as e:
            print(f"  Upload failed: {e}")


if __name__ == '__main__':
    fetch_and_upload_tmart_orders()
