# ClusterURL

The goal of this package is to provide a simple way to process and cluster a URL to extract a generic page route from it. These generic routes can then be used to aggregate data from multiple URLs under a single route.

This is done by a combination of a rules-based parser combined with a ML model trained to determine if a string is [gibberish](https://www.merriam-webster.com/dictionary/gibberish) or not.