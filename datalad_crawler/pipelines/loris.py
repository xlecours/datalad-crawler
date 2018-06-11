# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""A pipeline for crawling a crcns dataset"""

# Import necessary nodes
from ..nodes.crawl_url import crawl_url
from datalad.utils import updated
from ..nodes.annex import Annexificator
from datalad_crawler.consts import ARCHIVES_SPECIAL_REMOTE, DATALAD_SPECIAL_REMOTE


import json
# Possibly instantiate a logger if you would like to log
# during pipeline creation
from logging import getLogger
lgr = getLogger("datalad.crawler.pipelines.kaggle")

class LorisAPIExtractor(object):
    def __init__(self, apibase=None):
        self.apibase = apibase

    def __call__(self, data):
        jsdata = json.loads(data["response"])
        for candidate in jsdata["Images"]:
            yield updated(data, { "url" : self.apibase + candidate["Link"] })
        return

def pipeline(url=None, apibase=None):
    """Pipeline to crawl/annex a simple web page with some tarballs on it
    
    If .gitattributes file in the repository already provides largefiles
    setting, none would be provided here to calls to git-annex.  But if not -- 
    README* and LICENSE* files will be added to git, while the rest to annex
    """
    if apibase == None:
        raise "Wtf"

    lgr.info("Creating a pipeline to crawl data files from %s", url)
    annex = Annexificator(create=False,
            statusdb='json',
            special_remotes=[DATALAD_SPECIAL_REMOTE],
            options=["-c",
                "annex.largefiles="
                "exclude=Makefile and exclude=LICENSE* and exclude=ISSUES*"
                " and exclude=CHANGES* and exclude=README*"
                " and exclude=*.[mc] and exclude=dataset*.json"
                " and exclude=*.txt"
                " and exclude=*.json"
                " and exclude=*.tsv"
    ])

    return [
            crawl_url(url),
            LorisAPIExtractor(apibase),
            annex,
    ]
