#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018-2021 Raymon de Looff <raydelooff@gmail.com>
# This extension is open-source software licensed under the GPLv3 license.


__version__ = 0.5

from __future__ import absolute_import
import sys
import syslog
import backoff
from copy import copy
from datetime import datetime
from json import dumps as json_dumps
from re import sub as re_sub
from socket import gethostname

from weeutil.weeutil import to_bool, to_int
from weewx import NEW_ARCHIVE_RECORD, NEW_LOOP_PACKET
from weewx.manager import get_manager_dict_from_config
from weewx.restx import (AbortedPost, FailedPost, RESTThread, StdRESTful,
                         check_enable)
from weewx.units import to_METRICWX

# deal with differences between python 2 and python 3
try:
    # Python 3
    import queue
except ImportError:
    # Python 2
    # noinspection PyUnresolvedReferences
    import Queue as queue


class MQTTException(Exception):
    "MQTTException."


class MQTT(StdRESTful):
    """
    Publishes weather data from WeeWX to an MQTT broker.
    """
    def __init__(self, engine, config_dict):
        super(MQTT, self).__init__(engine, config_dict)

        if check_enable(config_dict, 'MQTT') is None:
            return

        mqtt_config_dict = config_dict['StdRESTful']['MQTT']
        mqtt_config_dict.pop('enable')

        client_id = mqtt_config_dict.pop(
            'client_id', gethostname())

        if 'binding' in mqtt_config_dict:
            manager_dict = get_manager_dict_from_config(
                config_dict, mqtt_config_dict.pop('binding'))
        else:
            manager_dict = None

        default_qos = mqtt_config_dict.pop('default_qos', 0)
        default_retain = mqtt_config_dict.pop('default_retain', False)

        self.loop_queue = queue.Queue()
        loop_topic_format = mqtt_config_dict.pop('loop_topic_format',
                                                 'weewx/loop/%s')
        loop_qos = mqtt_config_dict.pop('loop_qos', default_qos)
        loop_retain = mqtt_config_dict.pop('loop_retain', default_retain)

        self.archive_queue = queue.Queue()
        archive_topic_format = mqtt_config_dict.pop('archive_topic_format',
                                                    'weewx/archive/%s')
        archive_qos = mqtt_config_dict.pop('archive_qos', default_qos)
        archive_retain = mqtt_config_dict.pop('archive_retain', default_retain)

        observation_configs = mqtt_config_dict.pop('observations')

        try:
            self.loop_thread = MQTTThread(
                self.loop_queue,
                '%s-loop' % client_id,
                observation_configs=observation_configs,
                default_qos=loop_qos,
                default_retain=loop_retain,
                topic_format=loop_topic_format,
                manager_dict=manager_dict,
                **mqtt_config_dict)

            self.archive_thread = MQTTThread(
                self.archive_queue,
                '%s-archive' % client_id,
                observation_configs=observation_configs,
                default_qos=archive_qos,
                default_retain=archive_retain,
                topic_format=archive_topic_format,
                manager_dict=manager_dict,
                **mqtt_config_dict)
        except TypeError as e:
            syslog.syslog(
                syslog.LOG_ERR,
                "MQTT: Invalid values set in configuration: %s"
                % e)

            return

        self.loop_thread.start()
        self.bind(NEW_LOOP_PACKET, self.new_loop_packet)

        self.archive_thread.start()
        self.bind(NEW_ARCHIVE_RECORD, self.new_archive_record)

    def new_loop_packet(self, event):
        self.loop_queue.put(event.packet)

    def new_archive_record(self, event):
        self.archive_queue.put(event.record)


class MQTTThread(RESTThread):
    def __init__(self,
                 queue,
                 client_id,
                 host='localhost',
                 port=1883,
                 keepalive=60,
                 username=None,
                 password=None,
                 protocol_version='5',
                 ca_path=None,
                 tls_insecure=False,
                 observation_configs=None,
                 default_qos=0,
                 default_retain=False,
                 topic_format='%s',
                 manager_dict=None,
                 post_interval=None,
                 max_backlog=sys.maxsize,
                 stale=None,
                 log_success=False,
                 log_failure=True,
                 timeout=10,
                 max_tries=3,
                 retry_wait=5,
                 retry_login=3600,
                 softwaretype='weewx-mqtt-%s' % __version__,
                 skip_upload=False):
        """
        Constructor.
        """
        super(MQTTThread, self).__init__(queue,
                                         protocol_name='MQTT',
                                         manager_dict=manager_dict,
                                         post_interval=post_interval,
                                         max_backlog=max_backlog,
                                         stale=stale,
                                         log_success=log_success,
                                         log_failure=log_failure,
                                         timeout=timeout,
                                         max_tries=max_tries,
                                         retry_wait=retry_wait,
                                         retry_login=retry_login,
                                         softwaretype=softwaretype,
                                         skip_upload=skip_upload)

        self.topic_format = topic_format
        self.host = host
        self.port = to_int(port)
        self.keepalive = to_int(keepalive)
        self.observation_configs = observation_configs

        self.default_observation_config = {
            'qos': to_int(default_qos),
            'retain': to_bool(default_retain)
        }

        self.mqtt_client = self.create_client(client_id, protocol_version)
        self.configure_client(self.mqtt_client, username, password,
                              ca_path, to_bool(tls_insecure))

    def create_client(self, client_id, protocol_version):
        "Create the MQTT client."
        import paho.mqtt.client as mqtt

        if protocol_version == '3.1':
            protocol = mqtt.MQTTv31
        elif protocol_version == '3.1.1':
            protocol = mqtt.MQTTv311
        elif protocol_version == '5':
            protocol = mqtt.MQTTv5
        else:
            protocol = mqtt.MQTTv5

        mqtt_client = mqtt.Client(client_id, userdata=None, protocol=protocol)

        return mqtt_client

    def configure_client(self, mqtt_client, username=None, password=None,
                         ca_path=None, tls_insecure=False):
        "Configure the MQTT client."
        if username is not None:
            mqtt_client.username_pw_set(username, password)

        if ca_path is not None:
            mqtt_client.tls_set(ca_path)
            mqtt_client.tls_insecure_set(tls_insecure)

        if self.log_success is True and self.log_failure is True:
            mqtt_client.enable_logger()

        return mqtt_client

    def connect_client(self):
        "Connect to the MQTT broker."
        syslog.syslog(
            syslog.LOG_INFO,
            "%s: Trying to connect to broker: %s on port %d..."
            % (self.protocol_name, self.host, self.port))

        def on_connect(client, userdata, flags, return_code):
            from paho.mqtt.client import connack_string

            return_status = connack_string(return_code)

            if return_code == 0:
                syslog.syslog(
                    syslog.LOG_INFO,
                    "%s: Connected to broker '%s' on port %d: %s"
                    % (self.protocol_name, self.host, self.port, return_status))
            else:
                syslog.syslog(
                    syslog.LOG_ERR,
                    "%s: Could not connect to broker '%s' on port %d: %s"
                    % (self.protocol_name, self.host, self.port, return_status))

        self.mqtt_client.on_connect = on_connect

        try:
            self.mqtt_client.connect(self.host, self.port, self.keepalive)
            self.mqtt_client.loop_start()
        except Exception as e:
            raise MQTTException("Error connecting to broker: %s" % e)

        return self.mqtt_client

    def run(self):
        "Run the thread and disconnect the MQTT client on shutdown."
        while True:
            try:
                self.connect_client()
                break
            except MQTTException as e:
                syslog.syslog(
                    syslog.LOG_ERR,
                    "%s: Could not connect to broker '%s' on port %d: %s"
                    % (self.protocol_name, self.host, self.port, e))

                continue

        super(MQTTThread, self).run()

        def on_disconnect(client, userdata, flags, return_code):
            from paho.mqtt.client import connack_string

            return_status = connack_string(return_code)

            if return_code == 0:
                syslog.syslog(
                    syslog.LOG_INFO,
                    "%s: Successfully disconnected from broker '%s': %s"
                    % (self.protocol_name, self.host, return_status))
            else:
                syslog.syslog(
                    syslog.LOG_ERR,
                    "%s: Unexpected disconnection from broker '%s': %s"
                    % (self.protocol_name, self.host, return_status))

        self.mqtt_client.on_disconnect = on_disconnect

        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def process_record(self, packet, dbmanager):
        "Process record and publish to MQTT broker."
        # First, get the full record by querying the database ...
        if dbmanager is not None:
            record = self.get_record(packet, dbmanager)
        else:
            record = copy(packet)

        # ... then convert it to a proper unit system ...
        record = to_METRICWX(record)
        # ... then pop the timestamp ...
        timestamp = record.pop('dateTime')

        if self.skip_upload:
            raise AbortedPost()

        try:
            self.publish_record(timestamp, record)
        except Exception as e:
            raise FailedPost(e)

    def publish_record(self, timestamp, record):
        "Publish the given record to the MQTT broker."
        formatted_timestamp = datetime.utcfromtimestamp(timestamp)

        payload = {
            'timestamp': formatted_timestamp.isoformat() + '+00:00',
            'unix_timestamp': timestamp
        }

        for observation, value in record.iteritems():
            observation_config = self._get_observation_config(observation)
            observation_output_name = observation_config.get(
                'output_name', self._format_observation_type(observation))
            qos = to_int(observation_config.get('qos'))
            retain = to_bool(observation_config.get('retain'))

            topic = self.topic_format % observation_output_name
            payload['observation'] = observation_output_name
            payload['value'] = value

            try:
                self._mqtt_publish(topic, payload, qos, retain)
            except MQTTException as e:
                raise FailedPost(e)

    @backoff.on_exception(backoff.expo,
                          MQTTException)
    def _mqtt_publish(self, topic, payload, qos, retain):
        """
        Try to publish the given payload up to
        the configured number of tries.
        """
        import paho.mqtt.client as mqtt
        from paho.mqtt import MQTTException as PahoMQTTException

        payload_json = json_dumps(payload)

        for _count in range(self.max_tries):
            try:
                message_info = self.mqtt_client.publish(
                    topic, payload_json, qos, retain)

                if message_info.rc == mqtt.MQTT_ERR_SUCCESS:
                    return
                elif qos == 0:
                    return
                elif message_info.rc == mqtt.MQTT_ERR_AGAIN:
                    continue

                error = self._parse_return_code(message_info.rc)

                syslog.syslog(
                    syslog.LOG_DEBUG,
                    "%s: Error: %s"
                    % (self.protocol_name, error))

                raise MQTTException(
                    "Publish to topic '%s' failed: %s"
                    % (topic, error))
            except PahoMQTTException as e:
                syslog.syslog(
                    syslog.LOG_DEBUG,
                    "%s: Error: %s"
                    % (self.protocol_name, e))

                raise MQTTException(
                    "Could not publish message"
                    " to topic '%s': %s"
                    % (topic, e))

        # Failed to publish message. If QoS is set to zero, ignore it.
        if qos == 0:
            return

        raise MQTTException("Publish to topic '%s' failed" % topic)

    def _get_observation_config(self, observation):
        """
        Returns the config for the given observation. Defaults
        to the default observation config.
        """
        if self.observation_configs is None:
            return self.default_observation_config

        observation_config = self.observation_configs.get(observation)

        if observation_config is None:
            return self.default_observation_config

        config = self.default_observation_config.copy()
        config.update(observation_config)

        return config

    def _format_observation_type(self, observation):
        "Formats the observation type to the preferred format type."
        if self.observation_configs is None:
            return observation

        name_format = self.observation_configs.get('output_name_format')

        if name_format == 'snake_case':
            s1 = re_sub('(.)([A-Z][a-z]+)', r'\1_\2', observation)
            return re_sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

        return observation

    def _parse_return_code(self, return_code):
        "Parse the given MQTT error code into an error constant."
        from paho.mqtt.client import error_string

        error = error_string(return_code)

        return '%s (return code %s)' % (error, return_code)
