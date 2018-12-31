#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018-2019 Raymon de Looff <raydelooff@gmail.com>
# This extension is open-source software licensed under the GPLv3 license.


__version__ = 0.1

import syslog
from copy import copy
from datetime import datetime
from json import dumps as json_dumps
from Queue import Queue
from re import sub as re_sub
from socket import gethostname
from sys import maxint

from paho.mqtt import MQTTException

from weeutil.weeutil import to_bool, to_int
from weewx import NEW_ARCHIVE_RECORD, NEW_LOOP_PACKET
from weewx.manager import get_manager_dict_from_config
from weewx.restx import (AbortedPost, FailedPost, RESTThread, StdRESTful,
                         check_enable)
from weewx.units import to_METRICWX


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

        if 'binding' in mqtt_config_dict:
            manager_dict = get_manager_dict_from_config(
                config_dict, mqtt_config_dict.get('binding'))
        else:
            manager_dict = None

        default_qos = mqtt_config_dict.pop('default_qos', 0)
        default_retain = mqtt_config_dict.pop('default_retain', False)

        self.loop_queue = Queue()
        loop_topic_format = mqtt_config_dict.pop('loop_topic_format',
                                                 'weewx/loop/%s')
        loop_qos = mqtt_config_dict.pop('loop_qos', default_qos)
        loop_retain = mqtt_config_dict.pop('loop_retain', default_retain)

        self.archive_queue = Queue()
        archive_topic_format = mqtt_config_dict.pop('archive_topic_format',
                                                    'weewx/archive/%s')
        archive_qos = mqtt_config_dict.pop('archive_qos', default_qos)
        archive_retain = mqtt_config_dict.pop('archive_retain', default_retain)

        observation_configs = mqtt_config_dict.pop('observations', dict())

        try:
            self.loop_thread = MQTTThread(
                self.loop_queue,
                loop_topic_format,
                loop_qos,
                loop_retain,
                observation_configs=observation_configs,
                manager_dict=manager_dict,
                **mqtt_config_dict)

            self.archive_thread = MQTTThread(
                self.archive_queue,
                archive_topic_format,
                archive_qos,
                archive_retain,
                observation_configs=observation_configs,
                manager_dict=manager_dict,
                **mqtt_config_dict)
        except TypeError as e:
            syslog.syslog(
                syslog.LOG_ERR,
                "restx: MQTT: Invalid values set in configuration: %s"
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
                 topic_format,
                 default_qos=0,
                 default_retain=False,
                 client_id=None,
                 host='localhost',
                 port=1883,
                 keepalive=60,
                 username=None,
                 password=None,
                 protocol='3.1.1',
                 ca_path=None,
                 tls_insecure=False,
                 observation_configs=None,
                 manager_dict=None,
                 post_interval=None,
                 max_backlog=maxint,
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
        self.observation_configs = observation_configs or dict()

        self.current_values = dict()
        self.default_observation_config = {
            'qos': to_int(default_qos),
            'retain': to_bool(default_retain)
        }

        self.mqtt_client = self.create_client(client_id, protocol)
        self.configure_client(self.mqtt_client, username, password,
                              ca_path, to_bool(tls_insecure))

    def create_client(self, client_id, protocol_version):
        "Create the MQTT client."
        import paho.mqtt.client as mqtt

        if client_id is None:
            client_id = '%s@%s' % (self.softwaretype, gethostname())

            syslog.syslog(
                syslog.LOG_DEBUG,
                "restx: %s: Using generated client ID: %s"
                % (self.protocol_name, client_id))

        if protocol_version == '3.1.1':
            protocol = mqtt.MQTTv311
        elif protocol_version == '3.1':
            protocol = mqtt.MQTTv31
        else:
            protocol = mqtt.MQTTv311

        mqtt_client = mqtt.Client(client_id, clean_session=False,
                                  userdata=None, protocol=protocol)

        return mqtt_client

    def configure_client(self, mqtt_client, username=None, password=None,
                         ca_path=None, tls_insecure=None):
        "Configure the MQTT client."
        if username is not None:
            mqtt_client.username_pw_set(username, password)

        if ca_path is not None:
            mqtt_client.tls_set(ca_path)

        if tls_insecure is True:
            mqtt_client.tls_insecure_set(True)

        if self.log_success is True and self.log_failure is True:
            mqtt_client.enable_logger()

        return mqtt_client

    def connect_client(self):
        "Connect to the MQTT broker."
        syslog.syslog(
            syslog.LOG_INFO,
            "restx: %s: Trying to connect to broker: %s on port %s..."
            % (self.protocol_name, self.host, self.port))

        def on_connect(client, userdata, flags, return_code):
            if self.log_failure is True and return_code != 0:
                syslog.syslog(
                    syslog.LOG_ERR,
                    "restx: %s: Could not connect to broker: %s on port %s "
                    "(return code: %d)"
                    % (self.protocol_name, self.host, self.port, return_code))

        self.mqtt_client.on_connect = on_connect

        self.mqtt_client.connect(self.host, self.port, self.keepalive)
        self.mqtt_client.loop_start()

        return self.mqtt_client

    def run(self):
        "Run the thread and disconnect the MQTT client on shutdown."
        self.connect_client()

        super(MQTTThread, self).run()

        def on_disconnect(client, userdata, flags, return_code):
            if self.log_success is True and return_code == 0:
                syslog.syslog(
                    syslog.LOG_INFO,
                    "restx: %s: Succesfully disconnected from broker: %s "
                    "(return code: %d)"
                    % (self.protocol_name, self.host, return_code))

            if self.log_failure is True and return_code != 0:
                syslog.syslog(
                    syslog.LOG_ERR,
                    "restx: %s: Unexpected disconnection from broker: %s "
                    "(return code: %d)"
                    % (self.protocol_name, self.host, return_code))

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
        # ... then filter the differences ...
        record = self._diff_record(record)

        if self.skip_upload:
            raise AbortedPost()

        try:
            self.publish_record(timestamp, record)
        except ValueError as e:
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

            self._mqtt_publish(topic, payload, qos, retain)

    def _mqtt_publish(self, topic, payload, qos, retain):
        """
        Try to publish the given payload up to
        the configured number of tries.
        """
        try:
            payload_json = json_dumps(payload)
        except ValueError as e:
            raise FailedPost(e)

        syslog.syslog(
            syslog.LOG_DEBUG,
            "restx: %s: Publishing to topic: %s."
            "Payload: %s. QoS: %d. Retain: %s."
            % (self.protocol_name, topic, payload_json, qos, retain))

        for _count in range(self.max_tries):
            try:
                self.mqtt_client.publish(
                    topic, payload_json, qos, retain)

                # Return before raising an exception
                return
            except MQTTException:
                syslog.syslog(
                    syslog.LOG_DEBUG,
                    "restx: %s: Attempt %d. MQTT Exception: %s"
                    % (self.protocol_name, _count + 1, e))

        raise FailedPost(e)

    def _diff_record(self, record):
        """
        Returns the observations that are different
        than previous observations.
        """
        result = dict()

        for observation, value in record.iteritems():
            diff = self._diff_record_observation(observation, value)

            if diff is None:
                continue

            result[observation] = diff

        return result

    def _diff_record_observation(self, observation, value):
        """
        Returns the observation if the given value
        is not equal to the existing value. If the given
        value is equal to the existing value, None is returned.
        """
        current_value = self.current_values.get(observation, None)

        if current_value == value:
            return None

        self.current_values[observation] = value

        return value

    def _get_observation_config(self, observation):
        """
        Returns the config for the given observation. Defaults
        to the default observation config.
        """
        config = self.default_observation_config.copy()
        config.update(self.observation_configs.get(observation, dict()))

        return config

    def _format_observation_type(self, observation):
        name_format = self.observation_configs.get('output_name_format')

        if name_format == 'snake_case':
            return re_sub('([A-Z]+)', r'_\1', observation).lower()

        return observation
