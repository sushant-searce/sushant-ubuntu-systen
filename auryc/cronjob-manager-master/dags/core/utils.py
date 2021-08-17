# -*- coding: utf-8 -*-

import base64
import pendulum
from hashlib import blake2b


class Utils(object):

    @staticmethod
    def decode(source):
        if not source:
            return None
        decodestr = base64.b64decode(source)
        return decodestr.decode()

    @staticmethod
    def hash(*args):
        m = blake2b(digest_size=32)
        for arg in args:
            if arg:
                m.update(arg.encode('utf-8'))
        return m.hexdigest()

    @staticmethod
    def now():
        return pendulum.now('UTC')

    @staticmethod
    def start_date():
        return pendulum.now('UTC').subtract(minutes=120)

    @staticmethod
    def current_timestamp():
        return pendulum.now('UTC').int_timestamp*1000
