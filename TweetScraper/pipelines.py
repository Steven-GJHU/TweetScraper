# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
from scrapy.conf import settings
import logging
import pymongo
import json
import os
import TweetScraper.spiders.TweetCrawler as TweetCrawler

from TweetScraper.items import Tweet, User
from TweetScraper.utils import mkdirs

logger = logging.getLogger(__name__)


class SaveToMongoPipeline(object):
    ''' pipeline that save data to mongodb '''

    def __init__(self):
        connection = pymongo.MongoClient(settings['MONGODB_SERVER'], settings['MONGODB_PORT'])
        db = connection[settings['MONGODB_DB']]
        self.tweetCollection = db[settings['MONGODB_TWEET_COLLECTION']]
        self.userCollection = db[settings['MONGODB_USER_COLLECTION']]
        self.tweetCollection.ensure_index([('ID', pymongo.ASCENDING)], unique=True, dropDups=True)
        self.userCollection.ensure_index([('ID', pymongo.ASCENDING)], unique=True, dropDups=True)

    def process_item(self, item, spider):
        if isinstance(item, Tweet):
            dbItem = self.tweetCollection.find_one({'ID': item['ID']})
            if dbItem:
                pass  # simply skip existing items
                ### or you can update the tweet, if you don't want to skip:
                # dbItem.update(dict(item))
                # self.tweetCollection.save(dbItem)
                # logger.info("Update tweet:%s"%dbItem['url'])
            else:
                self.tweetCollection.insert_one(dict(item))
                logger.debug("Add tweet:%s" % item['url'])

        elif isinstance(item, User):
            dbItem = self.userCollection.find_one({'ID': item['ID']})
            if dbItem:
                pass  # simply skip existing items
                ### or you can update the user, if you don't want to skip:
                # dbItem.update(dict(item))
                # self.userCollection.save(dbItem)
                # logger.info("Update user:%s"%dbItem['screen_name'])
            else:
                self.userCollection.insert_one(dict(item))
                logger.debug("Add user:%s" % item['screen_name'])

        else:
            logger.info("Item type is not recognized! type = %s" % type(item))


class SaveToFilePipeline(object):
    ''' pipeline that save data to disk '''

    global savePath

    def __init__(self):
        self.saveTweetPath = settings['SAVE_TWEET_PATH']
        #self.saveUserPath = settings['SAVE_USER_PATH']
        mkdirs(self.saveTweetPath)  # ensure the path exists
        #mkdirs(self.saveUserPath)

    def process_item(self, item, spider):
        newsaveTweetPath = self.saveTweetPath + item['location'] + "/" + item['category'][:-4] + "/"
        mkdirs(newsaveTweetPath)
        filename = item['filename'][:-6]
        if isinstance(item, Tweet):
            self.savePath = os.path.join(newsaveTweetPath, filename)
            if os.path.isfile(self.savePath):
                self.append_to_file(item, self.savePath)# simply skip existing items
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.info("Update tweet:%s"%dbItem['url'])
            else:
                self.save_to_file(item, self.savePath)
                logger.debug("Add tweet:%s" % item['url'])

        elif isinstance(item, User):
            self.savePath = os.path.join(self.saveUserPath, item['ID'])
            if os.path.isfile(self.savePath):
                pass  # simply skip existing items
                ### or you can rewrite the file, if you don't want to skip:
                # self.save_to_file(item,savePath)
                # logger.info("Update user:%s"%dbItem['screen_name'])
            else:
                self.save_to_file(item, self.savePath)
                logger.debug("Add user:%s" % item['screen_name'])

        else:
            logger.info("Item type is not recognized! type = %s" % type(item))




    # def process_item(self, item, spider):
    #     if isinstance(item, Tweet):
    #         savePath = os.path.join(self.saveTweetPath, item['ID'])
    #         if os.path.isfile(savePath):
    #             pass  # simply skip existing items
    #             ### or you can rewrite the file, if you don't want to skip:
    #             # self.save_to_file(item,savePath)
    #             # logger.info("Update tweet:%s"%dbItem['url'])
    #         else:
    #             self.save_to_file(item, savePath)
    #             logger.debug("Add tweet:%s" % item['url'])
    #
    #     elif isinstance(item, User):
    #         savePath = os.path.join(self.saveUserPath, item['ID'])
    #         if os.path.isfile(savePath):
    #             pass  # simply skip existing items
    #             ### or you can rewrite the file, if you don't want to skip:
    #             # self.save_to_file(item,savePath)
    #             # logger.info("Update user:%s"%dbItem['screen_name'])
    #         else:
    #             self.save_to_file(item, savePath)
    #             logger.debug("Add user:%s" % item['screen_name'])
    #
    #     else:
    #         logger.info("Item type is not recognized! type = %s" % type(item))

    def save_to_file(self, item, fname):
        ''' input: 
                item - a dict like object
                fname - where to save
        '''
        item.pop('filename')
        item.pop('location')
        item.pop('category')
        with open(fname, 'w') as f:
            text = json.dumps(dict(item)) + ",\n"
            f.write(text)

    def append_to_file(self, item, fname):
        item.pop('filename')
        item.pop('location')
        item.pop('category')
        with open(fname, 'a') as f:
            text = json.dumps(dict(item)) + ",\n"
            f.write(text)
            #json.dump(dict(item), f) + ",\n"

