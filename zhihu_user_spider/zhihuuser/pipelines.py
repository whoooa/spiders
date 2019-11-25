# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import json



class MysqlPipeline(object):
    """
    采用同步的机制写入mysql
    """
    def __init__(self):
        self.conn = MySQLdb.connect(
            host='127.0.0.1',  # HOST
            user='root',  # USER
            password='niu123',  # PASSWORD
            db='mypro',  # DB_NAME
            charset="utf8")
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert ignore into mypro.yw_zhihu(answer_count, articles_count, follower_count, following_count, educations, description,
             locations, url_token, `name`, employments, business, user_type, headline, voteup_count, thanked_count, favorited_count, avatar_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(insert_sql, (item["answer_count"],
                                         item["articles_count"],
                                         item["follower_count"],
                                         item["following_count"],
                                         MySQLdb.escape_string(get_educations(item["educations"])),
                                         MySQLdb.escape_string(item["description"]),
                                         MySQLdb.escape_string(get_locations(item["locations"])),
                                         item["url_token"],
                                         MySQLdb.escape_string(item["name"]),
                                         MySQLdb.escape_string(get_employments(item["employments"])),
                                         MySQLdb.escape_string(item["business"]["name"]),
                                         item["user_type"],
                                         MySQLdb.escape_string(item["headline"]),
                                         item["voteup_count"],
                                         item["thanked_count"],
                                         item["favorited_count"],
                                         MySQLdb.escape_string(item["avatar_url"]),
                                         ))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        self.conn.close()

def get_employments(ems):
    if not ems:
        return ""
    return ";".join([i["company"]["name"]+"-"+i["job"]["name"] for i in ems if i ])

def get_locations(location):
    if not location:
        return ""
    return ",".join([i["name"] for i in location if i])


def get_educations(educations):
    if not educations:
        return ""
    e = []
    for i in educations:
        shool_name = i["school"]["name"] if i.get("shool") else ""
        major_name = i["major"]["name"] if i.get("major") else ""
        e.append(f"{shool_name}-{major_name}")
    return ";".join(e)