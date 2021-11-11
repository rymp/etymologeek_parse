#!/bin/python3
# core
from datetime import datetime
from os.path import dirname, abspath, join
from time import sleep
import uuid
import yaml
# additional
from bs4 import BeautifulSoup
from loguru import logger
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.options import Options
from sqlalchemy import create_engine, exc
from tqdm import tqdm

logger.add('file_{time}.log', format="{time} {level} {message}")


class Parser:
    """
    etymologeek.com parser for greater good!
    This module create a fake browser request for etymology history of the word.
    Main method - fit-transform requires word and language and returns list data.
    Please close parser in the end.
    """

    def __init__(self):
        profile = webdriver.FirefoxProfile()
        options = Options()
        options.headless = False
        options.add_argument("--enable-javascript")
        self.driver = webdriver.Firefox(profile, options=options)
        self.driver.set_window_size(1920, 1080)

    def fit_transform(self, word, language):
        """
        Returns data from etymology dictionary: word, language, definition, graph, descendants
        :param word: str
        :param language: str
        :return: list of [str, str, str, list of [list of [str, str]], list of str]
        """

        def graph_parse(source):
            """Returns edgelist as a list of [str and str] from graph object"""
            soup = BeautifulSoup(source, 'html.parser')
            edges = soup.find_all('g', 'edge')
            edgelist = [edge.find('title').text.strip().split('->') for edge in edges]
            return edgelist

        def table_parse(source):
            """Returns word, language and definition of ancestors from page's table"""
            res_tr = []
            soup = BeautifulSoup(source, 'html.parser')
            trs = soup.find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                td_res = []
                for td in tds:
                    if td.find('a'):
                        td_res.append(td.find('a')['href'])
                    else:
                        td_res.append(td.text.strip())
                res_tr.append(td_res)
            return res_tr

        def descendants_parse(source):
            """Return list of descendants from common root(s). Advantage in parse expand"""
            soup = BeautifulSoup(source, 'html.parser')
            return [a['href'] for a in soup.find_all('a', href=True)]

        def multiple_parse(source):
            """Returns links to the different etymology pages of homonyms"""
            soup = BeautifulSoup(source, 'html.parser')
            return ['/'.join(a['href'].split('/')[-2:]) for a in soup.find_all('a', href=True)]

        url = f"https://etymologeek.com/{language}/{word}"
        self.driver.get(url)
        sleep(1)
        # check empty data
        if self.driver.find_element_by_xpath('//*[@id="dtld"]').text == 'Page Not Found':
            return {'error': []}
        definition = self.driver.find_element_by_xpath('/html/body/section/div[1]/p').text.strip()
        # check if multiple words
        if 'We have found multiple words' in definition:
            return {'multiple': multiple_parse(self.driver.find_element_by_id('tb').get_attribute('innerHTML'))}
        res = table_parse(self.driver.find_element_by_id('tb').get_attribute('innerHTML'))
        try:
            descendants = descendants_parse(self.driver.find_element_by_id('or').get_attribute('innerHTML'))
        except NoSuchElementException:
            descendants = []
        self.driver.switch_to.frame(self.driver.find_element_by_id('pi'))
        graph = graph_parse(self.driver.find_element_by_id('graph0').get_attribute('innerHTML'))
        res.append([word, language, definition, graph, descendants])
        return {'ok': res}

    def close(self):
        """Close webdriver"""
        self.driver.close()


class Connector:
    """Class that open connection and allows dataframe import into db"""

    def __init__(self):
        self.basedir = dirname(abspath(__file__))
        with open(join(dirname(self.basedir), 'conf', 'db.yaml')) as f:
            db_yaml = yaml.load(f, Loader=yaml.FullLoader)
            self.pg_engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(
                user=db_yaml['user'],
                password=db_yaml['password'],
                host=db_yaml['host'],
                port=int(db_yaml['port']),
                database=db_yaml['database'])
            )

    @logger.catch
    def save_data(self, data):
        """
        Method save data into database table
        :param data: pandas.core.frame.DataFrame
        """

        try:
            data.to_sql(schema='etymology', name='vocabulary', con=self.pg_engine,
                        index=False, if_exists='append', method='multi')
        except exc.IntegrityError:
            pass


class Pipeline:
    """Class goes through word corpus, parse data and store"""

    def __init__(self):
        self.basedir = dirname(abspath(__file__))
        self.parser = Parser()
        self.conn = Connector()
        self.voc = pd.read_csv(join(dirname(self.basedir), 'data/actual_vocab.csv'), names=['word'])['word'].tolist()

    def fit(self):
        """Method start parsing words from vocabulary"""
        for word in tqdm(self.voc):
            # logging
            logger.info(f"{word}")
            res = self.parser.fit_transform(word=word, language="deu")
            if 'ok' in res:
                logger.info(f"{word} - ok")
                data = pd.DataFrame(res['ok'], columns=['word', 'language', 'definition', 'graph', 'descendants'])
                data['set_id'] = str(uuid.uuid4())
                data['upload'] = datetime.now().strftime('%Y-%m-%d')
                data = data[['set_id', 'word', 'language', 'definition', 'graph', 'descendants', 'upload']]
                self.conn.save_data(data)
                logger.info(f"{word} - saved")
            elif 'multiple' in res:
                logger.info(f"{word} - multiple")
                self.voc.extend(res['multiple'])
            else:
                logger.info(f"{word} - absence")
                pass
        self.parser.close()


if __name__ == '__main__':
    Pipeline().fit()
    # parser = Parser()
    # print(parser.fit_transform(word="Kampf", language="deu"))
    # parser.close()
