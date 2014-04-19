#!/usr/bin/env python
from __future__ import print_function

import gevent.monkey
gevent.monkey.patch_all()
import gevent

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

requests_log = logging.getLogger('requests');
requests_log.setLevel(logging.WARNING)

import requests
import re
import sqlite3
import datetime

from BeautifulSoup import BeautifulSoup, SoupStrainer
from urlparse import urlparse
import webbrowser
import urllib2
import os
import sys

#from Queue import Queue
from gevent.queue import Queue
q = Queue()


# Table syntax:
#   create table googls (url varchar(255) unique, date datetime, found_url varchar(255));
conn = sqlite3.connect('googls.db')

crawled_urls = set()
checked_youtube_annotation_ids = set()
checked_googls = set()
checked_images = set()


def modified(date_modified, allowed_margin):
    """
    Returns True if and only if the date is within the allowed
    margin of days of the current date.

    Parameters
    ----------
    date_modified (string): The date to test ("Tue, 15 Nov 1994 12:45:26 GMT")
    allowed_margin (int): The allowed number of days

    Returns
    -------
    True if the modified date is within the correct range
    """
    margin = datetime.timedelta(days=allowed_margin)
    today = datetime.date.today()

    modified_date = datetime.datetime.strptime(
        date_modified, "%a, %d %b %Y %H:%M:%S %Z").date()
    return today - margin <= modified_date <= today + margin


class Crawler(gevent.Greenlet):
    recursive = True
    download_images = False

    _base_domain_regex = r'(?:youtube\.com|(?:plus|developers)\.google\.com|android\.com|chrome\.com)'

    def __init__(self, q, conn):
        self._q = q
        self._conn = conn
        self._cursor = conn.cursor()

        gevent.Greenlet.__init__(self)

    def run(self):
        while True:
            try:
                q = self._q.get()
                if isinstance(q, tuple):
                    url, self._base_domain_regex = q
                else:
                    url = q
                    self._base_domain_regex = Crawler._base_domain_regex
            except StopIteration:
                log.info('Got StopIteration')
                break
            self.crawl(url)

    def test_googl(self, url):
        """ Tests goo.gl url to see if it's an invite """

        if not 'goo.gl' in url:
            log.warning('Got bad GOOGL: "%s"', url)
            return

        if not url.startswith('http://'):
            url = 'http://%s' % url

        global checked_googls
        if url in checked_googls:
            #log.debug('Skipping GOOGL: "%s"', url)
            return

        r = requests.get(url, allow_redirects=False, verify=False)
        checked_googls.add(url)

        if r.status_code != 200:
            return False

        loc = r.headers.get('location')

        if loc and loc.startswith('http://developers.google.com/events/io'):

            ts = datetime.datetime.now()

            try:
                self._cursor.execute(
                    '''INSERT INTO googls (url, date, found_url)
                       VALUES (?, ?, ?)''',
                    (url, ts, self._cur_url))
                self._conn.commit()

                log.error('Found GOOGL NEW: "%s"', url)
                webbrowser.open(url, new=2)
            except sqlite3.IntegrityError:
                log.error('Found GOOGL USED: "%s"', url)

            return True
        else:
            return False

    def test_googls(self, *urls):
        for u in urls:
            self.test_googl(u)

    def find_googls(self, text):
        m = re.findall(r'goo.gl/\w{6}', text)
        if m:
            #for i in m:
            #    yield i
            self.test_googls(*m)

    def fix_relative_child_url(self, url):
        if url.startswith('http'):
            return url
        elif url.startswith('/'):
            u = urlparse(self._cur_url)
            url = '%s://%s%s' % (u.scheme, u.hostname, url)
            #log.debug('Fixed relative /url "%s" using "%s"', url, self._cur_url)
        elif url.startswith('./'):
            u = urlparse(self._cur_url)
            url = '%s://%s%s%s' % (u.scheme, u.hostname, u.path, url[1:])
            #log.debug('Fixed relative ./url "%s" using "%s"', url, self._cur_url)
        elif url.startswith('//'):
            u = urlparse(self._cur_url)
            url = '%s:%s' % (u.scheme, url)
        else:
            u = urlparse(self._cur_url)
            path = u.path
            if not path.endswith('/'):
                path += '/'
            url = '%s://%s%s%s' % (u.scheme, u.hostname, path, url)
        return url

    def find_urls_in_html(self, html):
        for link in BeautifulSoup(html, parseOnlyThese=SoupStrainer('a')):
            href = link.get('href')
            if href:
                href = self.fix_relative_child_url(href)
                yield href

    def find_images_in_html(self, html):
        for link in BeautifulSoup(html, parseOnlyThese=SoupStrainer('img')):
            src = link.get('src')
            if src:
                src = self.fix_relative_child_url(src)
                yield src

    def fix_youtube_url(self, url):
        #m = re.search(r'(?:https?://(?:www\.)?youtube.com/)?/?(?:watch\?)?(?:v=)?([-_A-z0-9]+)', url)
        m = re.search(r'^(?:https?://(?:www\.)?youtube.com/)?/?(?:watch\?)?(?:v=)?([-_A-z0-9]+)$', url)
        if m:
            url = m.groups()[0]
            return url

    def check_youtube_annotations(self, url):
        video_id = self.fix_youtube_url(url)
        if not video_id:
            return

        global checked_youtube_annotation_ids
        if video_id in checked_youtube_annotation_ids:
            return

        #log.debug('Checking video annotations for: "%s"', video_id)

        annot_url = 'https://www.youtube.com/annotations_invideo?features=1&legacy=1&video_id=%s' % video_id
        r = requests.get(annot_url)
        self.find_googls(r.text)

        checked_youtube_annotation_ids.add(video_id)

    def crawl(self, url):
        #m = re.search(r'(?:^\/+(?:www\.)google.com|account)', url)
        #if m:
        #    log.debug('Skipping URL: "%s"', url)
        #    return

        m = re.search(self._base_domain_regex, url)
        if not m:
            log.debug('Skipping URL: "%s"', url)
            return

        if url in crawled_urls:
            #log.debug('Already crawled URL: "%s"', url)
            return

        self._cur_url = url
        crawled_urls.add(url)
        log.info('Crawling: "%s"', url)

        self.check_youtube_annotations(url)

        try:
            r = requests.get(url, verify=False)
        except Exception as e:
            log.error('Exception: %s', e)
            return

        html = r.text.encode('utf-8')

        self.find_googls(html)

        if self.recursive:
            try:
                urls = set(self.find_urls_in_html(html))
            except UnicodeEncodeError as e:
                log.error('Exception while finding URLs in "%d": %s', url, e)

            for child in urls:
                self._q.put(child)

        if self.download_images:
            last_modified = r.headers.get('last-modified')
            if not last_modified \
               or modified(last_modified, 2):
                try:
                    images = set(self.find_images_in_html(html))
                except UnicodeEncodeError as e:
                    log.error('Exception while finding URLs in "%d": %s',
                              url, e)

                for child in images:
                    for image in images:
                        self.check_image(image)

    def check_image(self, url):
        try:
            if url in checked_images:
                #log.error('Skipping image: "%s"', url)
                return
            fn = 'img/%s' % url.split('/')[-1]
            if not os.path.isfile(fn):
                #r = urllib2.urlopen(url)
                r = requests.get(url, verify=False, stream=True)
                if r.status_code == 200:
                    last_modified = r.headers.get('last-modified')
                    if not last_modified \
                       or modified(last_modified, 2):
                        with open(fn, 'wb') as f:
                            for chunk in r.iter_content(1024):
                                f.write(chunk)
        except Exception as e:
            log.error('Cannot download image "%s": %s', url, e)


def crawl_one(url):
    c = Crawler(q, conn)
    c.start()

    q.put((url, ''))

    #gevent.wait()


crawlers = []


def crawl_pool(num):
    global crawlers
    for _ in range(num):
        c = Crawler(q, conn)
        c.start()
        crawlers.append(c)


def put(args):
    global crawlers
    if not crawlers:
        crawl_pool(20)
    q.put(args)
    gevent.sleep(10000000000)


if __name__ == '__main__':
    url = sys.argv[1]
    if len(sys.argv) > 2:
        base = sys.argv[2]
        put((url, base))
    else:
        put(url)
