import json
import re
import ssl
import paho.mqtt.client as mqtt
import getpass
import xml.etree.ElementTree as ET
from collections import deque
import datetime
from datetime import datetime as DT
from fints.client import FinTS3PinTanClient, NeedTANResponse
from fints.hhd.flicker import terminal_flicker_unix
from fints.utils import minimal_interactive_cli_bootstrap
import os
a=0
I2=0
iban_liste = (("DE86120300001024119347", "Tagesgeld" ), ("DE92120300001033666270", "Girokonto"))

# MQTT-Konfiguration
MQTT_BROKER = 'your_home_assitant_url'
MQTT_PORT = 8883  # Standardport für MQTT über SSL
MQTT_TOPIC_PREFIX = 'homeassistant/sensor/fints'
MQTT_USERNAME = 'username'
MQTT_PASSWORD = 'password'
MQTT_CA_CERT = 'ca_vert'

# FinTS Konfiguration
BLZ = '12030000'
LOGIN = 'your_username'
PIN = getpass.getpass('PIN:')
URL = 'https://banking-dkb.s-fints-pt-dkb.de/fints30'
PRODUCT_ID = '6151256F3D4F9975B877BD4A2'

f = FinTS3PinTanClient(BLZ, LOGIN, PIN, URL, product_id=PRODUCT_ID)

def calculate_balance(nested_xml_data, include_pending=False):
    balance = 0.0
    namespace = {'ns': 'urn:iso:std:iso:20022:tech:xsd:camt.052.001.02'}
    for xml_data_tuple in nested_xml_data:
        for xml_data_bytes in xml_data_tuple:
            if xml_data_bytes is not None:
                root = ET.fromstring(xml_data_bytes)
                booked_balances = root.findall('.//ns:Bal', namespace)
                if booked_balances:
                    last_booked_balance = booked_balances[-1]
                    balance = float(last_booked_balance.find('ns:Amt', namespace).text)
                if include_pending:
                    pending_entries = root.findall('.//ns:Ntry[ns:Sts="PDNG"]', namespace)
                    for entry in pending_entries:
                        if entry.find('ns:ValDt/ns:Dt', namespace)==None:
                            amount = float(entry.find('ns:Amt', namespace).text)
                            indicator = entry.find('ns:CdtDbtInd', namespace).text
                            if indicator == 'DBIT':
                                balance -= amount
                            elif indicator == 'CRDT':
                                balance += amount
    return round(balance, 2)

def get_last_10_transactions(account,iban,Account_Name):
    transactions = deque(maxlen=10)
    i = 0
    while len(transactions) != transactions.maxlen:
        i += 60
        if i == 60:
            res = f.get_transactions_xml(account, datetime.date.today() - datetime.timedelta(days=i), datetime.date.today() - datetime.timedelta(days=i-60))
        else:
            res = f.get_transactions_xml(account, datetime.date.today() - datetime.timedelta(days=i), datetime.date.today() - datetime.timedelta(days=i-59))
        while isinstance(res, NeedTANResponse):
            
            res = ask_for_tan(res)
        namespace = {'ns': 'urn:iso:std:iso:20022:tech:xsd:camt.052.001.02'}
        for xml_data_tuple in reversed(res):
            for xml_data_bytes in reversed(xml_data_tuple):
                if xml_data_bytes is not None:
                    root = ET.fromstring(xml_data_bytes)
                    entries = root.findall('.//ns:Ntry', namespace)
                    for entry in reversed(entries):
                        amount = entry.find('ns:Amt', namespace).text
                        indicator = entry.find('ns:CdtDbtInd', namespace).text
                        if indicator == 'DBIT':
                            amount = float('-' + amount)
                        else:
                            amount = float('+' + amount)
                        status = entry.find('ns:Sts', namespace).text
                        booking_date = entry.find('ns:BookgDt/ns:Dt', namespace).text
                        booking_date=DT.strptime(booking_date,'%Y-%m-%d')
                        booking_date=booking_date.strftime('%d-%m-%Y')
                        valuation_date = entry.find('ns:ValDt/ns:Dt', namespace)
                        if valuation_date is not None:
                            valuation_date = valuation_date.text
                            valuation_date=DT.strptime(valuation_date,'%Y-%m-%d')
                            valuation_date=valuation_date.strftime('%d-%m-%Y')
                        else:
                            valuation_date="PDNG"
                        details = entry.find('ns:NtryDtls/ns:TxDtls', namespace)
                        recording_time = details.find('ns:Refs/ns:Prtry/ns:Ref', namespace)
                        if recording_time is not None:
                            recording_time = recording_time.text
                        else:
                            recording_time = None
                        sendername = details.find('ns:RltdPties/ns:Dbtr/ns:Nm', namespace)
                        if sendername is not None:
                            if sendername.text == 'ISSUER':
                                sendername = 'Justin Hahn'
                            else:
                                sendername = sendername.text
                        else:
                            sendername = 'DKB'
                        receivername = details.find('ns:RltdPties/ns:UltmtCdtr/ns:Nm', namespace)
                        if receivername is not None:
                            receivername = receivername.text
                        else:
                            receivername = details.find('ns:RltdPties/ns:Cdtr/ns:Nm', namespace)
                            if receivername is not None:
                                receivername = receivername.text
                            else:
                                receivername = 'Justin Hahn Tagesgeld'
                        comments = []
                        comment_elements = details.findall('ns:RmtInf/ns:Ustrd', namespace)
                        if comment_elements is not None:
                            for comment_element in comment_elements:
                                comment_text = re.sub(r'\s+', ' ', comment_element.text.strip())
                                if comment_text is not None:
                                    comments.append(comment_text)
                            if comments:
                                comment = '\n'.join(comments).replace('\n', ' ')
                            else:
                                comment = "None"
                        global I2
                        I2=I2+1
                        if Account_Name is not None:
                            transaction = {
                                'amount': amount,
                                'status': status,
                                'booking_date': booking_date,
                                'valuation_date': valuation_date,
                                'recording_time': recording_time,
                                'comment': comment,
                                'sendername': sendername,
                                'receivername': receivername,
                                'Eigene_Iban' : iban,
                                'Eigener_Accountname' : Account_Name,
                                'position' : I2
                            }
                        else:
                                transaction = {
                                'amount': amount,
                                'status': status,
                                'booking_date': booking_date,
                                'valuation_date': valuation_date,
                                'recording_time': recording_time,
                                'comment': comment,
                                'sendername': sendername,
                                'receivername': receivername,
                                'Eigene_Iban' : iban,
                                'position' : I2
                            }
                        transactions.append(transaction)
                        if len(transactions) == 10:
                            transactions_list = list(transactions)
                            return transactions_list    
    transactions_list = list(transactions)
    return transactions_list

def get_balance_with(account):
    res = f.get_transactions_xml(account, datetime.date.today() - datetime.timedelta(days=0), datetime.date.today())
    while isinstance(res, NeedTANResponse):
        res = ask_for_tan(res)
    balance = calculate_balance(res)
    balance_with_pending = calculate_balance(res, include_pending=True)
    return balance, balance_with_pending

def ask_for_tan(response):
    print("A TAN is required")
    print(response.challenge)
    if getattr(response, 'challenge_hhduc', None):
        try:
            terminal_flicker_unix(response.challenge_hhduc)
        except KeyboardInterrupt:
            pass
    tan = input('Please enter TAN:')
    return f.send_tan(response, tan)

def send_mqtt_discovery_message(client, iban, balance, balance_with_pending, transactions,idx,Account_Name):
    global a
    b={
    'iban': iban,}
    if a == 0:
        client.tls_set(ca_certs=MQTT_CA_CERT, certfile=None, keyfile=None, tls_version=ssl.PROTOCOL_TLSv1_2)
        a = 1
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    model= None
    if Account_Name!=None:
        model=Account_Name
        b={ 'IBAN': iban,'account_name' : Account_Name}
    else:
        model=iban
    # Geräteeinträge definieren
    summary_device = {
        "name": f"DKB Zusammenfassung {model}",
        "identifiers": f"dkb_summary_{model}",
        "manufacturer": "Justin Hahn",
        "model": model
    }

    activities_device = {
        "name": f"DKB Aktivitäten {model}",
        "identifiers": f"dkb_activities_{model}",
        "manufacturer": "Justin Hahn",
        "model": model
    }

    # Home Assistant MQTT Discovery für den Kontostand ohne ausstehende Buchungen
    discovery_topic_balance = f"homeassistant/sensor/dkb_summary_{iban}/balance/config"
    summary_attributes_topic = f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance/attributes"

    balance_payload = {
        "unique_id": f"_balance_without_pending_{idx+1}",
        "name": f" Kontostand ohne geplante Buchungen",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance/state",
        "unit_of_measurement": "EUR",
        "value_template": "{{ value_json.balance }}",
        "json_attributes_topic": f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance/attributes",
        "device": summary_device
    }
    client.publish(discovery_topic_balance, json.dumps(balance_payload), qos=0, retain=True)
    client.publish(summary_attributes_topic, json.dumps(b), qos=0, retain=True)
    
   
    # MQTT-Nachricht mit dem Kontostand ohne ausstehende Buchungen senden
    balance_state_topic = f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance/state"
    client.publish(balance_state_topic, json.dumps({"balance": balance}), qos=0, retain=True)

    # Home Assistant MQTT Discovery für den Kontostand mit ausstehenden Buchungen
    discovery_topic_balance_with_pending = f"homeassistant/sensor/dkb_summary_{iban}/balance_with_pending/config"
    summary_attributes_topic = f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance_with_pending/attributes"
    balance_with_pending_payload = {
        "unique_id": f"_balance_with_pending_{idx+1}",
        "name": f" Kontostand mit geplanten Buchungen",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance_with_pending/state",
        "unit_of_measurement": "EUR",
        "json_attributes_topic": f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance_with_pending/attributes",
        "value_template": "{{ value_json.balance }}",
        "device": summary_device
    }
    client.publish(discovery_topic_balance_with_pending, json.dumps(balance_with_pending_payload), qos=0, retain=True)
    a=json.dumps(b)
    client.publish(summary_attributes_topic, json.dumps(b), qos=0, retain=True)

    # MQTT-Nachricht mit dem Kontostand mit ausstehenden Buchungen senden
    balance_with_pending_state_topic = f"{MQTT_TOPIC_PREFIX}/dkb_summary_{iban}/balance_with_pending/state"
    client.publish(balance_with_pending_state_topic, json.dumps({"balance": balance_with_pending}), qos=0, retain=True)

    # Home Assistant MQTT Discovery für jede Transaktion
    for idx_tx, transaction in enumerate(transactions):
        discovery_topic_transaction = f"homeassistant/sensor/dkb_transaction_{iban}_{idx_tx}/config"
        transaction_payload = {
            "unique_id": f"dkb_transaction_{iban[-2:]}{idx_tx}",
            "name": f"DKB Transaktion_{idx_tx+1}",
            "state_topic": f"{MQTT_TOPIC_PREFIX}/dkb_transaction_{iban}/{idx_tx}/state",
            "value_template": "{{ value_json.amount }}",
            "unit_of_measurement": "EUR",
            "json_attributes_topic": f"{MQTT_TOPIC_PREFIX}/dkb_transaction_{iban}/{idx_tx}/attributes",
            "device": activities_device
        }
        client.publish(discovery_topic_transaction, json.dumps(transaction_payload), qos=0, retain=True)

        # MQTT-Nachricht für jede Transaktion senden
        transaction_state_topic = f"{MQTT_TOPIC_PREFIX}/dkb_transaction_{iban}/{idx_tx}/state"
        client.publish(transaction_state_topic, json.dumps({"amount": transaction['amount']}), qos=0, retain=True)

        transaction_attributes_topic = f"{MQTT_TOPIC_PREFIX}/dkb_transaction_{iban}/{idx_tx}/attributes"
        a=json.dumps(transaction)
        client.publish(transaction_attributes_topic, json.dumps(transaction), qos=0, retain=True)

    client.disconnect()



def main():
    minimal_interactive_cli_bootstrap(f)
    mqtt_client = mqtt.Client()
    with f:
        if f.init_tan_response:
            ask_for_tan(f.init_tan_response)
        accounts = f.get_sepa_accounts()
        if isinstance(accounts, NeedTANResponse):
            accounts = ask_for_tan(accounts)
        for idx, account in enumerate(accounts):
            iban= account.iban
            Account_Name= None
            if iban_liste is not None:
                for item in iban_liste:
                    if iban==item[0]:
                        Account_Name=item[1]
            global I2
            I2=0
            balance, balance_with_pending = get_balance_with(account)
            transactions = get_last_10_transactions(account,iban,Account_Name)
            send_mqtt_discovery_message(mqtt_client, iban, balance, balance_with_pending, transactions,idx,Account_Name)
            print(f"Data for account {idx+1} sent to MQTT and registered in Home Assistant")

if __name__ == "__main__":
    main()
