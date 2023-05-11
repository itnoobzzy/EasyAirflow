#!/usr/bin/env python
# -*- coding:utf-8 -*-


import base64


def str_encryption(need_str):
    en_str = base64.b64encode(bytes(str(need_str), 'utf-8')).decode("utf-8")

    return en_str

def str_decryption(en_str):
    de_str = base64.b64decode(bytes(str(en_str), 'utf-8')).decode("utf-8")

    return de_str