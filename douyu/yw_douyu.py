import requests
import MySQLdb
import time

def main_name(p):
    url = "https://www.douyu.com/betard/%s" % p
    page = requests.get(url)
    try:
        print(page.status_code)
        data = page.json()
    except:
        return

    room = data.get("room")
    nick_name = room["nickname"] if room else ""
    room_id = room["room_id"] if room else ""
    cate_name = data["column"]["cate_name"] if data.get("column") else ""
    second_lvl_name = room["second_lvl_name"] if room else ""
    rome_url = room["room_url"] if room else ""
    od = (room["officialAnchor"].get("od") or "") if room else ""
    # seo_description = data["seo_info"]["seo_description"]
    # seo_keyword = data["seo_info"]["seo_keyword"]
    # seo_title = data["seo_info"]["seo_title"]
    zb_type = "dy"
    datas = (nick_name, room_id, cate_name, second_lvl_name, rome_url, od, zb_type)
    query = "insert ignore into mypro.yw_douyu(`nick_name`, rid, cate_name, second_lvl_name, rome_url, od,zb_type) values(%s,%s,%s,%s,%s,%s,%s)"
    into_mysql(query, tuple(datas))



def into_mysql(query, datas, many=False):
    conn = MySQLdb.connect(host="127.0.0.1", user="root", password="niu123", db="mypro", charset="utf8")
    cursor = conn.cursor()
    if many:
        cursor.executemany(query, datas)
    else:
        cursor.execute(query, datas)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    for i in range(961784, 1000000):
        print(i)
        try:
            main_name(i)
        except Exception as e:
            print(e)