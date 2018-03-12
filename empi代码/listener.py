# -*- coding: utf-8 -*-

import stomp
import logging

from update_empi_main import main

logging.basicConfig(level=logging.DEBUG)


class MyListener(stomp.ConnectionListener):
    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_message(self, headers, message):
        # print headers
        print('received a message %s' % message)
        if str(message) == 'sync finished this time':
            print 'Message received'
            main('zmap_r_patient')


conn = stomp.Connection([('123.232.38.100', 61613)])

conn.start()
conn.connect()

# 发送消息到主题
# conn.send(body='this is message', destination = '/topic/testTopic')
# 从队列接受消息
res = conn.subscribe(destination='empi_useless.data.queue', id=1, ack='auto')
# conn.send(body='hello activemq!', destination = '/empi_useless/data/queue')
print '***********************'
# 从主题接受消息
# conn.subscribe(destination='/topic/testTopic', id=1, ack='auto')
conn.set_listener('', MyListener())
print '*****'
# while 1:
#     time.sleep(2)

conn.disconnect()
