import psycopg2
import telebot
from telebot import types


DB_conf = {
    'database': 'alef_test',
    'user': 'postgres',
    'password': 'cydvbb3qkc',
    'host': 'localhost',
    'port': '5432'
}

BOT_conf = {
    'token': '5347977538:AAH6fEI5s_xpaGUPRaBRer4OGy2o6bOBl74',
}


def get_cities(text):
    conn = None
    print("Connecting to DB")
    try:
        conn = psycopg2.connect(**DB_conf)
    except:
        print("I am unable to connect to the database")

    cur = conn.cursor()

    cur.execute("SELECT * FROM cities WHERE name LIKE %s", ('%' + text + '%',))
    rows = cur.fetchall()

    cities = []
    for row in rows:
        cities.append({'name': row[1], 'id': row[0]})

    conn.close()

    return cities


bot_url = 'https://api.telegram.org/bot' + BOT_conf['token'] + '/'
bot = telebot.TeleBot(BOT_conf['token'])

@bot.message_handler(content_types=['text'])
def get_text(message):
    if message.text == "Hello":
        bot.send_message(message.from_user.id, text='Hello!')
    else:
        cities = get_cities(message.text.capitalize())
        if len(cities) > 0:
            print(cities)
            buttons = types.InlineKeyboardMarkup()
            for city in cities:
                buttons.add(types.InlineKeyboardButton(text=city['name'], callback_data=str(city['id'])))
            bot.send_message(message.from_user.id, text='Выберите город', reply_markup=buttons)
        else:
            print('No cities')
            bot.send_message(message.from_user.id, text='Городов не найдено')


@bot.callback_query_handler(func=lambda call: True)
def callback_inline(call):
    conn = None
    print("Connecting to DB")
    try:
        conn = psycopg2.connect(**DB_conf)
    except:
        print("I am unable to connect to the database")


    cur = conn.cursor()
    # get city for id
    cur.execute("SELECT * FROM cities WHERE id = %s", (call.data,))
    city = cur.fetchone()

    msg = f"Название - {city[1]}\nНаселение - {city[2]}\n{city[3]}"
    bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=msg)
    conn.close()

bot.polling(non_stop=True, interval=0)