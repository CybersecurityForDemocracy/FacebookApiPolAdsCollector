# crowdtangle-raw

A tool for pulling the raw results from Crowdtangle dashboards.

## installing

For one-off fetches, you can use poetry to install required dependencies via `poetry install`.

If you want to run it in a sustained fashion, you should also install [supervisor](http://supervisord.org/).

## running

It has two basic modes. If you'd just like to fetch one dashboard once, you can do

```shell
% poetry shell
% python fetch-and-save.py DASHBOARDTOKEN
```

where the token is found by going to the Crowdtangle dashboard clicking on the gear
in the upper-right corner, and picking "API Access".

That will dump all the posts for the last 7 days of the dashboard to stdout in ndjson
format.

If you are planning to run this regularly, you probably want to specify a directory
and optional tag so that this writes one timestamped file per run. For example:

```shell
% python fetch-and-save.py --directory saved --tag foo DASHBOARDTOKEN
```

will then write files like `foo-20221103-223210.ndjson.bz2` to the `saved` directory.

One way to make that happen is via `supervisord`. The `make-supervisord-conf.py`
program gives an example that will fetch many different dashboards in a loop, with
`supervisord` starting new jobs as the old ones finish.