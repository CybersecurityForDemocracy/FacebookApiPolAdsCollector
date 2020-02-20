## Introduction

Facebook launched their searchable archive of U.S. political advertisements on May 24, 2018. We devise a data collection methodology to obtain a large set of 267,000 political ads from Facebook’s political ad archive. We find that in total, ads with political content have generated at least 1,435,089,000 impressions and have cost their sponsors $13,913,300 and possibly up to 3,884,705,000 impressions and spent $71,754,827 on advertising with U.S. political content. Individual Facebook pages that sponsor political content ads, on average, generate at least 72,501 total impressions and spend at least \$703 on advertising. Facebook has also launched an API for programmatic access to this archive.

## Facebook Political Ad Archive Overview

According to Facebook: "The archive includes Facebook and Instagram ads that have been classified as containing political content, or content about national issues of public importance." This archive provides an increased level of transparency of political ads on Facebook and Instagram.

## Setup

1. Clone project and install packages, ideally using virtualenv
2. Create a new database using `sql/unified_schema.sql`
3. Create a config file `config.cfg` based on `config.cfg.EXAMPLE`
4. Collect some ads `python generic_fb_collector.py`
