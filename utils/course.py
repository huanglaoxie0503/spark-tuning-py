#!/usr/bin/python
# -*- coding:UTF-8 -*-


class CoursePay(object):
    def __init__(self, order_id, dis_count, pay_money, create_time, dt, dn):
        self.order_id = order_id
        self.dis_count = dis_count
        self.pay_money = pay_money
        self.create_time = create_time
        self.dt = dt
        self.dn = dn
