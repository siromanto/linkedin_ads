""" Pyhton test API client for LinkedIn

"""

import httplib2
import json
from urllib.parse import urlencode


class BingWebmasterApi:

    def __init__(self, timeout=20):
        # self.api_key = api_key or config.API_KEY
        # self.api_key = api_key
        self.endpoint = 'https://api.linkedin.com/v2/'
        self.h = httplib2.Http("/tmp/.cache", timeout=timeout)

    def __getattr__(self, item):
        def call(**kwargs):
            kwargs.update({
                'apikey': self.api_key
            })
            _uri = '{endpoint}{function}?{query}'.format(
                endpoint=self.endpoint,
                function=item,
                query=urlencode(kwargs)
            )
            resp, content = self.h.request(_uri, 'GET')

            if not resp.status == 200:
                if resp.status == 404:
                    print('Method does not exists!')
                elif resp.status == 400:
                    _c = json.loads(content)
                    _ec = _c.get('ErrorCode')
                    if _ec == 8:
                        print('Invalid parameter given to %s function.' % item)
                    elif _ec == 14:
                        print('Not authorized.')
                raise print('Unknown error occured [status code: %d] with response: %s' % (resp.status, content))
            else:
                return json.loads(content)

        return call
