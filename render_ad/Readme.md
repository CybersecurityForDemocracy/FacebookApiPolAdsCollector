Ad Renderer
===========

Configuration
-------------

The ad renderer code originally comes from a different codebase and has a separate configuration facility.

The configuration file is passed as a parameter to the `fb_ad_creative_retriever.py` script. For commonn configuration
keys that may need to be configured, see `./config/configuration.ini.template`. To see what else can be configured, all
configuration options and their default values are located in `./defaults.ini`. This file should not be modified for
configuration purposes. Instead, any default value can be overridden in the custom configuration file. On startup, the
ad renderer script logs which configuration file(s) are used, and their version. To keep track of configuration file
changes, be sure to update the version key at the top of the file.


### Secrets

The tool requires a valid Facebook API token to operate. Secrets such as the Facebook API key should be stored in a file
`./config/secrets.ini`. (The location of the secrets file can be changed by setting the `secrets_location` key in the
`[Config]` section of the configuration file.) The template `./config/secrets.ini.template` contains the configuration
keys that need to be set. Ensure that the file can be read by the crawler, but not by other users.


### Docker

The script renders ads using Selenium, a browser automation tool. By default, the browser is started automatically in a
Docker container (whereas the script itself runs outside the container). Several steps are necessary to make this work:

1. Building the Docker image: Use the command in the first line of `./docker/Dockerfile.browser` to build the Docker
   image with Selenium (and additional fonts to properly render non-English ads) and make it available locally.
2. Ensure the user running the `fb_ad_creative_retriever.py` script has sufficient permissions to start Docker 
   containers. On Linux, this usually means adding the user to the `docker` group. Note that this is somewhat equivalent
   to giving `root` permissions, so it is recommended to take appropriate security precautions (restricting access to
   that account, and if the ad rendering code is incorporated into a network-reachable service, performing additional
   hardening, etc.).

The default configuration should allow the script to start Docker containers for the browser when it is running outside
a container. When the script is to run within a Docker container, too (no such container is currently supplied by us),
the configuration needs to be changed. Read the source code or contact us to find out how.


Maintenance
-----------

Facebook sometimes updates the rendering page for ads, which may break the extraction of ad texts and images. It is
therefore recommended to regularly run the tests for the most basic ad extraction functionality in the parent directory
(`fb_ad_creative_retriever_test.py`). Typically, fixing (simple) errors due to layout/DOM structure changes (in the
easiest cases, renaming of CSS classes) means updating the XPATH definitions in `render_ad.py`. Note, in rare cases
Facebook has been observed to change the ads used in the tests (e.g., "ad removed" label toggled or image re-encoded),
which would require the tests to be updated.

To run the tests, create a configuration file `./config/fb_ad_creative_retriever_test.cfg` (within the `render_ad`
directory) and also create a corresponding secrets file. Set `raise_if_missing_image=false` or some tests using old ads
will fail.
