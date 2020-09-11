"""Centralized module for computing simhash of Ad creative text."""
import re

import simhash

_WIDTH = 3

# IF YOU CHANGE THIS FUNCTION MAKE SURE TO ALSO CHANGE
# FacebookAdsAnalysis/sim_hash_ad_creative_text.py AND REGENERATE ALL ad_creatives.text_sim_hash
# VALUES
def _get_features(src):
    src = src.lower()
    src = re.sub(r'[^\w]+', '', src)
    src = re.sub(r'(\A|\s)#(\w+)', '', src)
    src = re.sub(r'(\A|\s)@(\w+)', '', src)
    src = re.sub(r'__', '', src)
    return [src[i:i + _WIDTH] for i in range(max(len(src) - _WIDTH + 1, 1))]

def hash_ad_creative_text(text):
    return simhash.Simhash(_get_features(text)).value
