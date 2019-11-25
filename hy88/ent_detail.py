import MySQLdb

def get_qydata():
    # 取出总数
    # 循环取出获取每页20数量
    conn = MySQLdb.connect(host="127.0.0.1", user="root", password="niu123", db="mypro", charset="utf8")
    cursor = conn.cursor()
    cursor.execute("select `id`, `name`, `url` from mypro.yw_qy8803 where checked is null;")
    # 点击目录查到各个行业链接
    for (name, url) in cursor.fetchall():
        pageurl = url + f"pn{page}/"
        print(pageurl)
        time.sleep(0.3)
        save_pageqy(pageurl, name)