from bs4 import BeautifulSoup
import requests
import sqlite3 as sq
import time
import datetime

class ArrlSessionCount:
    STATES_DICT = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'DC': 'District Of Columbia', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'PR': 'Puerto Rico', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming', 'AS': 'American Samoa', 'AE': 'Armed Forces - Europe', 'AP': 'Armed Forces - Pacific', 'AA': 'Armed Forces - USA/Canada', 'FM': 'Federated States of Micronesia', 'GU': 'Guam', 'MH': 'Marshall Islands', 'MP': 'Northern Mariana Islands', 'PW': 'Palau', 'VI': 'Virgin Islands', 'Non-US': 'Non-US'}
    ARRL_URL = "http://www.arrl.org/ve-session-counts"
    DATABASE_PATH = '../ve-database/ve_session_counts.db'
    LOG_PATH = '../ve-database/log.txt'

    # Extract ve data from a row
    def __extract(self, row, state):
        ve_info ={
            'callSign' : None,
            'name' : None,
            'state' : state,
            'county' : None,
            'accreditation' : None,
            'sessions' : None
        }
        #gets callSign
        ve_info['callSign'] = row.td.b.string
        #gets name
        row.td.b.extract()
        ve_info['name'] = row.td.string.strip().replace("(", "").replace(")", "")
        #gets county
        ve_info['county'] = row.find_all('td')[1].string
        #gets accreditation
        ve_info['accreditation'] = row.find_all('td')[2].string
        #gets sessions
        ve_info['sessions'] = row.find_all('td')[3].string
        for key in ve_info:
            ve_info[key] = str(ve_info[key])
        return ve_info

    def __init__(self):
        # Gets a list of states available on the arrl ve session coutns page
        # r = requests.get(ArrlSessionCount.ARRL_URL)
        # doc = BeautifulSoup(r.content, 'html.parser')
        # for option in doc.find('select', {'name':'state'}).find_all('option'):
        #     if(option['value'] == ""):
        #         continue
        #     else:
        #         ArrlSessionCount.STATES_DICT[option['value']] = option.string
        #Initiates a database table if one doesn't exist
        self.conn = sq.connect(ArrlSessionCount.DATABASE_PATH)
        self.cursor = self.conn.cursor()
        # checks if the database exists
        try:
            self.cursor.execute('''CREATE TABLE ve_session_counts(
                callSign TEXT,
                name TEXT,
                state TEXT,
                county TEXT,
                accreditation TEXT,
                sessions TEXT
            )
            ''')
            self.conn.commit()
        except sq.OperationalError:
            return
        #gets all the ve info for every state and stores it to the database
        for key in ArrlSessionCount.STATES_DICT:
            r = requests.get((ArrlSessionCount.ARRL_URL+f"?state={key}"))
            doc = BeautifulSoup(r.content, 'html.parser')
            try:
                table = doc.table.find_all('tr')
            except AttributeError:
                continue
            table.pop(0) #removes the headers
            for row in table:
                ve_info = self.__extract(row, ArrlSessionCount.STATES_DICT[key])
                #Inserts into database
                with self.conn:
                    print(ve_info['callSign'], " ", ve_info['state'])
                    self.cursor.execute(f'''INSERT INTO ve_session_counts VALUES(
                        "{ve_info['callSign']}",
                        "{ve_info['name']}",
                        "{ve_info['state']}",
                        "{ve_info['county']}",
                        "{ve_info['accreditation']}",
                        "{ve_info['sessions']}"
                    )
                    ''')
            time.sleep(60)
        self.conn.close()

    def sync(self):
        dt = datetime.datetime.now()
        with open(ArrlSessionCount.LOG_PATH, 'a') as file:
            file.write(f"Sync in progress {dt.strftime('%c')}\n")
        for key in ArrlSessionCount.STATES_DICT:
            r = requests.get((ArrlSessionCount.ARRL_URL+f"?state={key}"))
            doc = BeautifulSoup(r.content, 'html.parser')
            try:
                table = doc.find("table", id="sc_table").find_all("tr")
            except AttributeError:
                continue
            table.pop(0) #removes the headers
            for row in table:
                ve_info = self.__extract(row, ArrlSessionCount.STATES_DICT[key])
                ve_info = tuple(value for value in ve_info.values())
                print(ve_info[0], ve_info[1])
                with self.conn:
                    self.cursor.execute('SELECT * FROM ve_session_counts WHERE callSign = ? AND state = ?', (ve_info[0], ve_info[2],))
                    existing_record = self.cursor.fetchone()
                    if existing_record is not None:
                        if existing_record != ve_info:
                            self.cursor.execute('''UPDATE ve_session_counts
                            SET name = ?, state = ?, county = ?, accreditation = ?, sessions = ?
                            WHERE callSign = ?
                            ''', (ve_info[1], ve_info[2], ve_info[3], ve_info[4], ve_info[5], ve_info[0]))
                            with open(ArrlSessionCount.LOG_PATH, 'a') as file:
                                file.write("VE Updated\n")
                                file.write(f"{existing_record}\n")
                                file.write(f"{ve_info}\n")
                    else:
                        self.cursor.execute(f'''INSERT INTO ve_session_counts VALUES(
                        "{ve_info[0]}",
                        "{ve_info[1]}",
                        "{ve_info[2]}",
                        "{ve_info[3]}",
                        "{ve_info[4]}",
                        "{ve_info[5]}"
                    )
                    ''')
            with open(ArrlSessionCount.LOG_PATH, 'a') as file:
                file.write(f"Fetched {ArrlSessionCount.STATES_DICT[key]}\n")
            time.sleep(60)
        self.conn.close()
        with open(ArrlSessionCount.LOG_PATH, 'a') as file:
            file.write("VE Stats Fetched!\n\n")

    def get_ve_stats(self, call_sign : str):
        call_sign = call_sign.upper()
        result = None
        with self.conn:
            self.cursor.execute('''SELECT * FROM ve_session_counts WHERE callSign = ?''', (call_sign,))
            result = self.cursor.fetchone()
        if result is not None:
            print(result)
            return result
        else:
            return None

if __name__ == '__main__':
    arrl = ArrlSessionCount()
    arrl.sync()