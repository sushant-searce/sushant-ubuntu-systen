import pymysql

def demo():

    list=[]
    conn = pymysql.connect(host='sushant-crisil-test.cih0jkvggyaj.us-east-2.rds.amazonaws.com',
                            user='admin',
                            password='password',
                            database='test',
                            port=3306)
    cur = conn.cursor()
    cur.execute("SELECT * FROM entries")
    data = cur.fetchone()   
    for r in cur:
        list.append(r)
        # print(r)
    return list
    # cur.close()
    # conn.close()

print(demo())