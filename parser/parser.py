import requests
import psycopg2
from bs4 import BeautifulSoup


DB_conf = {
    'database': 'alef_test',
    'user': 'postgres',
    'password': 'cydvbb3qkc',
    'host': 'localhost',
    'port': '5432'
}


def parse_wiki():
    url = "https://ru.wikipedia.org/wiki/%D0%93%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B8%D0%B5_%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D0%B5_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%8B_%D0%9C%D0%BE%D1%81%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%BE%D0%B9_%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D0%B8"
    r = requests.get(url)

    #create soup by class mw-parser-output
    soup = BeautifulSoup(r.text).findChildren("div", {"class": "mw-parser-output"})

    #find tables in soup
    city_tables = soup[0].findChildren("table", {"class": "standard sortable"})

    #create list of cities
    cities = []

    #get body from table
    for table in city_tables:
        body = table.findChildren("tbody")[0]
        #get rows from body
        for string in body:
            #get cells from rows
            cells = string.select("td")
            #create list of city dicts
            if len(cells) > 0:
                population = cells[4].text.replace("[4]", "").replace("↘", "").replace("↗", "").replace("\xa0", "")
                city = {
                    'name': cells[1].text,
                    'population': population,
                    'anchor': "ru.wikipedia.org" + cells[1].findChildren("a")[0].get("href")
                    }
                cities.append(city)

    return cities


def get_city(city_name):
    #connect to DB
    conn = None
    print("Connecting to DB")

    try:
        conn = psycopg2.connect(**DB_conf)
    except:
        print("I am unable to connect to the database")

    #create cursor
    cur = conn.cursor()

    #get city from DB
    cur.execute("SELECT * FROM cities WHERE name = %s", (city_name,))
    city = cur.fetchone()

    return city   


def save_cities():
    #connect to DB
    conn = None
    try:
        conn = psycopg2.connect(**DB_conf)
    except:
        print("I am unable to connect to the database")

    #create cursor
    cur = conn.cursor()

    #check if table exists
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cities')")
    if cur.fetchone()[0] == 0:
        cur.execute("CREATE TABLE cities (id serial PRIMARY KEY, name varchar(255), population varchar(255), anchor varchar(255))")
        conn.commit()
        print("Table created")

    created_cities = []
    updated_cities = []

    #save cities to DB
    for city in parse_wiki():
        if get_city(city['name']) is None:
            cur.execute("INSERT INTO cities (name, population, anchor) VALUES (%s, %s, %s)", (city['name'], city['population'], city['anchor']))
            created_cities.append(city['name'])
        else:
            cur.execute("UPDATE cities SET anchor = %s,  population = %s WHERE name = %s", (city['anchor'], city['population'], city['name']))
            updated_cities.append(city['name'])

    conn.commit()
    conn.close()
    
#execute
save_cities()