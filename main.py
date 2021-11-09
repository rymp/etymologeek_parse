#!/bin/python3
# core
from time import sleep
# additional
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


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
        Returns data from etymology dictionary: word, language, definition, graph, descendants, is_instance
        :param word: str
        :param language: str
        :return: list of [str, str, str, list of [list of [str, str]], list of str, bool]
        """

        def graph_parse(source):
            """ Returns edgelist as a list of [str and str] from graph object"""
            soup = BeautifulSoup(source, 'html.parser')
            edges = soup.find_all('g', 'edge')
            edgelist = [edge.find('title').text.split('->') for edge in edges]
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
                        td_res.append(td.text)
                res_tr.append(td_res)
            return res_tr

        def descendants_parse(source):
            """Return list of descendants from common root(s). Advantage in parse expand"""
            soup = BeautifulSoup(source, 'html.parser')
            return [a['href'] for a in soup.find_all('a', href=True)]

        def multiple_parse(source):
            """Returns links to the different etymology pages of homonyms"""
            soup = BeautifulSoup(source, 'html.parser')
            return [a['href'] for a in soup.find_all('a', href=True)]

        url = f"https://etymologeek.com/{language}/{word}"
        self.driver.get(url)
        sleep(1)
        # check empty data
        if self.driver.find_element_by_xpath('//*[@id="dtld"]').text == 'Page Not Found':
            return {'error': []}
        definition = self.driver.find_element_by_xpath('/html/body/section/div[1]/p').text
        # check if multiple words
        if 'We have found multiple words' in definition:
            return {'multiple': [word,
                                 language,
                                 multiple_parse(self.driver.find_element_by_id('tb').get_attribute('innerHTML'))]
                    }
        else:
            res = [table_parse(self.driver.find_element_by_id('tb').get_attribute('innerHTML'))]
            if self.driver.find_element_by_id('or'):
                descendants = descendants_parse(self.driver.find_element_by_id('or').get_attribute('innerHTML'))
            else:
                descendants = []
            self.driver.switch_to.frame(self.driver.find_element_by_id('pi'))
            graph = graph_parse(self.driver.find_element_by_id('graph0').get_attribute('innerHTML'))
            res.append([word, language, definition, graph, descendants, True])
            return {'ok': res}

    def close(self):
        """Close webdriver"""
        self.driver.close()
