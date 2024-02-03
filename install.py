#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018-2024 Raymon de Looff <raydelooff@gmail.com>
# This extension is open-source software licensed under the GPLv3 license.

__version__ = 0.5


from setup import ExtensionInstaller


def loader():
    "Installs the WeeWX MQTT extension."
    return MQTTInstaller()


class MQTTInstaller(ExtensionInstaller):
    "Installs the WeeWX MQTT extension."

    def __init__(self):
        super(MQTTInstaller, self).__init__(
            version=__version__,
            name='mqtt',
            description='Send weather data to an MQTT broker.',
            author='Raymon de Looff',
            author_email='raydelooff@gmail.com',
            restful_services='user.mqtt.MQTT',
            config={
                'StdRESTful': {
                    'MQTT': {
                        'enable': 'True',
                        'host': 'localhost',
                        'port': '1883',
                        'username': 'weewx',
                        'password': 'weewx'
                    }
                }
            },
            files=[('bin/user', ['bin/user/mqtt.py'])]
        )
