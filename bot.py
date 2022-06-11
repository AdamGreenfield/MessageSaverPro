import os
import sqlite3
import discord
import time
import re
import random
from dotenv import load_dotenv
import threading
from datetime import date

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GOD = os.getenv('GOD_ID')
BOT_ID = os.getenv('BOT_ID')

intents = discord.Intents.default()
intents.members = True
client = discord.Client(intents=intents)


def initialize_db(cur):
    cur.execute('CREATE TABLE IF NOT EXISTS users (userid int UNIQUE, username text);')
    cur.execute(
        'CREATE TABLE IF NOT EXISTS history (messageid int, wordstripped text, word text, hash int, userid int, date date, position int, UNIQUE(messageid, position));')
    cur.execute('CREATE TABLE IF NOT EXISTS statistics (date date, time real);')
    cur.execute('CREATE INDEX IF NOT EXISTS history_hash ON history(hash);')
    cur.execute('CREATE INDEX IF NOT EXISTS history_messageidpos ON history(messageid, position);')
    cur.execute('CREATE INDEX IF NOT EXISTS history_pos ON history(position);')


@client.event
async def on_ready():
    await client.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name='-help'))
    print('Ready')


@client.event
async def on_message(message):
    if not message.content.startswith('-'):
        return
    try:
        dbName = 'messages_' + str(message.guild.id) + '.db'
        con = sqlite3.connect(dbName)
        cur = con.cursor()
        initialize_db(cur)
        cur.execute('PRAGMA synchronous=OFF')
        cur.execute('PRAGMA journal_mode=MEMORY')
        if message.content.startswith('-load'):
            await message.channel.send('Loading')
            print('Loading')
            timeS = time.time()
            cur.execute('begin')
            for member in message.guild.members:
                cur.execute('INSERT OR IGNORE INTO users (userid, username) VALUES (?,?);', (member.id, member.name))
            pattern = re.compile('\W')
            for channelCurr in message.guild.text_channels:
                permissions = channelCurr.permissions_for(discord.utils.get(channelCurr.guild.members, name='MessageSaverPro'))
                if permissions.read_message_history & permissions.read_messages:
                    async for messageCurr in channelCurr.history(limit=200000):
                        if str(messageCurr.author.id) != str(BOT_ID) and not messageCurr.content.startswith('-build'):
                            i = 0
                            for wordCurr in messageCurr.content.split():
                                cur.execute('''INSERT OR IGNORE INTO history (messageid, wordstripped, word, hash, userid, date, position)
                                               VALUES (?,?,?,?,?,?,?);''', (
                                messageCurr.id, re.sub(pattern, '', wordCurr), wordCurr, hash(wordCurr),
                                messageCurr.author.id, messageCurr.created_at, i))
                                i += 1
                    print(f'{channelCurr.name} completed')
            cur.execute('commit')
            print(f'{(time.time() - timeS) / 60}')
            print('Success')
            await message.channel.send('Success')
        elif message.content.startswith('-build'):
            split = message.content.split()
            name = []
            id = -1
            start_time = time.time()
            if len(split) > 1:
                for i in range(1, len(split)):
                    name.append(split[i].lower())
                id = cur.execute('SELECT userid FROM users WHERE lower(username)=? UNION ALL SELECT -1;',
                                 (' '.join(name),)).fetchone()[0]
            print(f'Building for server {message.guild.name} ID {id}')
            builder = SentenceBuilder(con, cur, ' '.join(name), id)
            sentence = builder.build_sentence()
            await message.channel.send(' '.join(sentence))
            cur.execute('begin')
            cur.execute('INSERT INTO statistics(date, time) VALUES (?,?)', (date.today(), time.time() - start_time))
            cur.execute('commit')
            print('Done')
        elif message.content.startswith('-help'):
            await message.channel.send('```-load - Must be used first\n-build - Build a random sentence\n-build [discord username] - Build random sentence for specified user\n-stats - Some interesting stats about the server\n-lookup [word] - Get # of occurnces of word```')
        elif message.content.startswith('-stats'):
            totalmessages = cur.execute('SELECT COUNT(DISTINCT messageid) FROM history;').fetchone()[0]
            topusers = []
            topwords = []
            for row in cur.execute('''  SELECT U.username, COUNT(DISTINCT H.messageid) AS c 
                                        FROM history H 
                                            INNER JOIN users U ON U.userid = H.userid 
                                        GROUP BY U.username 
                                        ORDER BY C DESC LIMIT 10;'''):
                topusers.append([row[0], row[1]])
            #I'll fix this later...
            for row in cur.execute('''  SELECT word, COUNT(*) AS c 
                                        FROM history 
                                        WHERE LOWER(word) NOT IN (  "the","i","a","to","you","is","it","my","and","in",
                                                                    "that","of","me","on","just","have","for","this",
                                                                    "was","like","what","so","do","get","be","with",
                                                                    "your","im","no","at","we","are","can","not","up",
                                                                    "but","he","all","if","one","some","they","go","its",
                                                                    "yeah","out","how", "got", "dont", "when","about",
                                                                    "i'm","-","-play", "by", "will", "now","u","as",
                                                                    "or","?play","from","added","has","an","us")
                                        GROUP BY word 
                                        ORDER BY c DESC LIMIT 10;'''):
                topwords.append([row[0], row[1]])
            sendmessage = '```Total Messages: {}\n\nTop users:\n'.format(totalmessages)
            for i in range(len(topusers)):
                sendmessage += '{}. {}: {} messages\n'.format(i+1, topusers[i][0], topusers[i][1])
            sendmessage += '\nTop words:\n'
            for i in range(len(topwords)):
                sendmessage += '{}. {}: {} occurences\n'.format(i+1, topwords[i][0], topwords[i][1])
            sendmessage += '```'
            await message.channel.send(sendmessage)
        elif message.content.startswith('-lookup'):
            word = message.content.split()[1]
            occurences = cur.execute('SELECT COUNT(*) FROM history WHERE word = ?;', (word,)).fetchone()[0]
            await message.channel.send('```"{}" occurences: {}```'.format(word, occurences))
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))


def hash(string):
    string = string.lower()
    ascii = ''
    for ch in string:
        ascii += str(ord(ch))

    return (int(ascii) * 7907) % 2147483647


class SentenceBuilder:
    con = None
    cur = None
    id = -1
    name = ''

    def __init__(self, con, cur, name, id):
        self.con = con
        self.cur = cur
        self.name = name
        self.id = int(id)

    def build_sentence(self):
        sentence = []
        list = []
        weights = []
        hashes = {}
        hashCurr = 0
        try:
            if self.id > 0:
                sentence.append(self.name + ':')
                for row in self.cur.execute(
                        'SELECT H.word, H.hash, COUNT(*) AS count FROM history H WHERE H.position = 0 AND H.userid = ? GROUP BY H.hash ORDER BY count DESC;',
                        (self.id,)):
                    list.append(row[0])
                    weights.append(row[2])
                    hashes[row[0]] = row[1]
            else:
                for row in self.cur.execute(
                        'SELECT H.word, H.hash, COUNT(*) AS count FROM history H WHERE H.position = 0 GROUP BY H.hash ORDER BY count DESC;'):
                    list.append(row[0])
                    weights.append(row[2])
                    hashes[row[0]] = row[1]
            word = random.choices(list, weights=weights, k=1)[0]
            hashCurr = hashes[word]

            sentence.append(word)
            pos = 1
            while word != '':
                list.clear()
                weights.clear()
                if self.id > 0:
                    for row in self.cur.execute('''SELECT H.word, H.hash, COUNT(*) AS count FROM history H 
                                                    INNER JOIN history H1 ON H1.messageid = H.messageID AND H1.position = ? AND H1.hash = ?
                                                    WHERE H.position = ? AND H.userid = ? GROUP BY H.hash
                                                    ORDER BY COUNT DESC;''',
                                                (pos - 1, hashCurr, pos, self.id)):
                        list.append(row[0])
                        weights.append(row[2])
                        hashes[row[0]] = row[1]
                else:
                    for row in self.cur.execute('''SELECT H.word, H.hash, COUNT(*) AS count FROM history H 
                                                    INNER JOIN history H1 ON H1.messageid = H.messageID AND H1.position = ? AND H1.hash = ?
                                                    WHERE H.position = ? GROUP BY H.hash
                                                    ORDER BY COUNT DESC;''',
                                                (pos - 1, hashCurr, pos)):
                        list.append(row[0])
                        weights.append(row[2])
                        hashes[row[0]] = row[1]
                if len(list) > 0:
                    word = random.choices(list, weights=weights, k=1)[0]
                    hashCurr = hashes[word]
                    sentence.append(word)
                else:
                    word = ''
                    break
                pos += 1
        except IndexError:
            return ['None']

        return sentence


client.run(TOKEN)