import requests
import calendar
import os
import shutil
import re
import json
import hashlib

from collections import defaultdict

from_sender_regex = re.compile('From\s([^\s]+\sat\s[^\.]+\.[a-z0-9]+)(.*)\nFrom\:\s[^\s]+\sat\s[^\.]+\.[a-z0-9]+',
                               re.MULTILINE|re.UNICODE)
message_id_regex = re.compile('Message\-ID\:\s+\<([^<>]+)\>')
reference_id_regex = re.compile('References\:\s+\<([^<>]+)\>')

def download_archives(start=2007, end=2019):
    """ Download all archives from given start and end """

    for y in range(start, end+1):
        for m in range(1, 13):
            month = calendar.month_name[m]
            url = 'https://mail.python.org/pipermail/bangpypers/{}-{}.txt.gz'.format(y, month)
            f = requests.get(url)

            if f.status_code == 200:
                print('Downloaded for {} {}'.format(month, y))
                fname = url.rsplit('/')[-1]
                open(fname, 'wb').write(f.content)
                os.system('gunzip {}'.format(fname))
            elif f.status_code == 404:
                print('Not found for {} {}'.format(month, y))

def classify_year(root='archives', start=2007, end=2019):
    """ Move the files and arrange according to years """

    for y in range(start, end+1):
        folder = os.path.join(root, str(y))
        if not os.path.isdir(folder): os.makedirs(folder)
        
        for m in range(1, 13):
            month = calendar.month_name[m]
            fname = '{}-{}.txt'.format(y, month)
            if os.path.isfile(fname):
                print('Moving {} to {}/{}'.format(fname, folder, fname))
                # Move it to the correct folder
                shutil.move(fname, folder)

def extract_email_stats(root='archives', start=2007, end=2019):
    """ Extract emails stats from archives """

    year_email_stats = defaultdict(int)
    month_email_stats = defaultdict(int)
    sender_stats = defaultdict(int)
    
    # Use "From <sender> at <origin><dot><tld>" as a regex to separate emails in text file
    for y in range(start, end+1):
        n_emails = 0
        for m in range(1, 13):
            month = calendar.month_name[m]
            fname = os.path.join(root, str(y), '{}-{}.txt'.format(y, month))
            if os.path.isfile(fname):
                data = open(fname, 'rb').read().decode('latin-1')
                from_parts = from_sender_regex.findall(data)
                n_emails = len(from_parts)
                senders = [x[0].replace(' at ','@').strip() for x in from_parts]
                for sender in senders:
                    sender_stats[sender] += 1
                    
                print('{} emails from {}'.format(n_emails, fname))
                month_email_stats['/'.join((str(y), str(m)))] = n_emails
                year_email_stats[str(y)] += n_emails

    json.dump(month_email_stats, open('global_stats_month.json','w'), indent=4)
    json.dump(year_email_stats, open('global_stats_year.json','w'), indent=4)
    json.dump(sender_stats, open('global_stats_sender.json','w'), indent=4)

def extract_emails(root='archives', years=range(2007, 2020)):
    """ Extract emails from archives - also builds thread statistics """

    thread_stats = defaultdict(int)
    all_threads = defaultdict(list)
    sender_to_msgid = {}
    
    # Use "From <sender> at <origin><dot><tld>" as a regex to separate emails in text file
    for y in years:
        n_emails = 0
        m_count = 0
        all_senders = []

        for m in range(1, 13):
            month = calendar.month_name[m]
            fname = os.path.join(root, str(y), '{}-{}.txt'.format(y, month))
            if os.path.isfile(fname):
                m_count += 1
                data = open(fname, 'rb').read().decode('latin-1')

                emails = []
                count = 0

                month_dir = os.path.join(root, str(y), month)
                if not os.path.isdir(month_dir):
                    os.makedirs(month_dir)
                    
                print('Processing for {}/{}'.format(y, month))
                while True:
                    m1 = from_sender_regex.search(data)
                    if m1 == None:
                        print('No further emails')
                        break
                    
                    idx1_1 = m1.start()
                    idx1_2 = m1.end()
                    # Start of next email
                    data2 = data[idx1_2:]

                    m2 = from_sender_regex.search(data2)
                    if m2 == None:
                        print('No next emails')
                        # Append data till now
                        email_data = data[idx1_1:]
                        emails.append(email_data)
                        count += 1
                        # print('{} Email, length => {}'.format(count, len(email_data)))                      
                        break

                    idx2_1 = m2.start()
                    # Email text is between these two indices
                    email_data = data[idx1_1:idx1_2] + data2[:idx2_1]
                    emails.append(email_data)
                    
                    count += 1
                    # print('{} Email, length => {}'.format(count, len(email_data)))
                    data = data2[m2.start():]

                for email_data in emails:
                    # Extract message-id
                    try:
                        msg_id = message_id_regex.findall(email_data)[0].strip()
                    except IndexError:
                        pass

                    from_parts = from_sender_regex.findall(email_data)
                    sender = from_parts[0][0].replace(' at ','@').strip()
                    sender_to_msgid[msg_id] = sender

                    msg_idh = hashlib.md5(msg_id.encode('utf-8')).hexdigest()
                    email_path = os.path.join(month_dir, msg_idh + '.eml')
                    print('Writing message {}'.format(email_path))
                    open(email_path, 'w').write(email_data)

                    # Figure out references
                    try:
                        references_id = reference_id_regex.findall(email_data)[0]
                        orig_sender = sender_to_msgid.get(references_id)

                        thread_key = '/'.join((str(y), month, references_id))
                        thread_stats[thread_key] += 1

                        if orig_sender != None:
                            coll = all_threads[thread_key]
                            # print('ORIGINAL SENDER =>',orig_sender)
                            if coll[0] != orig_sender:
                                print('Inserting orig sender',orig_sender)
                                coll.insert(0, orig_sender)

                        if sender != orig_sender:
                            all_threads[thread_key].append(sender)
                    except IndexError:
                        single_msg_key = '/'.join((str(y), month, msg_id))                          
                        all_threads[single_msg_key].append(sender)                          

        # Write global thread stats
        json.dump(thread_stats, open('global_thread_stats.json','w'), indent=4)
        # Write global thread graph
        json.dump(all_threads, open('global_thread_graph.json','w'), indent=4)     

if __name__ == "__main__":
    download_archives()
    classify_year()
    extract_email_stats()
    extract_emails()

