#! /usr/bin/python3

import copy
from data import get_record, get_unique_ip_count, get_count_per, get_count_per_src_dst_address, get_count_per_src_dst_mac_address
from enum import Enum
from nicegui import ui

class graph:
    class SumOrCount(Enum):
        bcount = True
        ncount = False
        none   = False

    def __init__(self, title, group_by, data_function, column, items, where, sum_bytes: SumOrCount):
        self.title = title
        self.group_by = group_by
        self.data_function = data_function
        self.column = column
        self.items = items
        self.where = where
        self.sum_bytes = sum_bytes

        fig_template = {
            'data': [
            ],
            'layout': {
                'margin': {'l': 30, 'r': 0, 't': 0, 'b': 15},
                'plot_bgcolor': '#E5ECF6',
                'xaxis': {'gridcolor': 'white'},
                'yaxis': {'gridcolor': 'white'},
            },
        }

        self.fig = copy.deepcopy(fig_template)

        for i in self.items:
            self.fig['data'].append({
                    'type': 'scatter',
                    'name': i[1],
                    'x': [],
                    'y': [],
                })

        if self.column:
            self.fig['data'].append({
                    'type': 'scatter',
                    'name': 'other',
                    'x': [],
                    'y': [],
                })

    def begin(self):
        with ui.card():
            ui.label(self.title)
            plot = ui.plotly(self.fig).classes('w-100 h-80')

        self.plot = plot
        self.update()

    @ui.refreshable
    def update(self):
        item_names = [v[0] for v in self.items]
        for iy in range(len(self.items)):
            self.fig['data'][iy]['x'].clear()
            self.fig['data'][iy]['y'].clear()
        if self.column:
            self.fig['data'][-1]['x'].clear()
            self.fig['data'][-1]['y'].clear()
            if self.sum_bytes == graph.SumOrCount.bcount:
                data = self.data_function(self.group_by, self.column, self.items, self.where, True)
            elif self.sum_bytes == graph.SumOrCount.ncount:
                data = self.data_function(self.group_by, self.column, self.items, self.where, False)
            for item in data:
                if item[2] in item_names:
                    nr = item_names.index(item[2])
                else:
                    nr = -1
                self.fig['data'][nr]['x'].append(item[0])
                self.fig['data'][nr]['y'].append(item[1])
        else:
            if self.sum_bytes == graph.SumOrCount.bcount:
                data = self.data_function(self.group_by, True)
            elif self.sum_bytes == graph.SumOrCount.ncount:
                data = self.data_function(self.group_by, False)
            for iy in range(len(self.items)):
                for item in data:
                    self.fig['data'][iy]['x'].append(item[0])
                    self.fig['data'][iy]['y'].append(item[1 + iy])
        self.plot.update()

ports = ((80, 'HTTP'), (443, 'HTTPS'), (5900, 'VNC'), (123, 'NTP'))
ip_protocols = ((1, 'ICMP'), (6, 'TCP'), (17, 'UDP'))

g_hour = []
g_hour.append(graph('record count', 'HOUR', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_hour.append(graph('unique IP address', 'HOUR', get_unique_ip_count, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_hour.append(graph('IP protocol counts', 'HOUR', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_hour.append(graph('destination port', 'HOUR', get_count_per, 'dst_port', ports, '(ip_protocol = 6 OR ip_protocol = 17)', graph.SumOrCount.none))
g_hour.append(graph('source/destination IP address pair', 'HOUR', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_hour.append(graph('source/destination MAC address pair', 'HOUR', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

g_hour_b = []
g_hour_b.append(graph('record byte sum', 'HOUR', get_record, None, (('n', 'n'),), None, graph.SumOrCount.bcount))

g_dow = []
g_dow.append(graph('record count', 'ISODOW', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_dow.append(graph('unique IP address', 'ISODOW', get_unique_ip_count, None, ('IPv4', 'IPv6',), None, graph.SumOrCount.none))
g_dow.append(graph('IP protocol counts', 'ISODOW', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_dow.append(graph('destination port', 'ISODOW', get_count_per, 'dst_port', ports, None, graph.SumOrCount.none))
g_dow.append(graph('source/destination IP address pair', 'ISODOW', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_dow.append(graph('source/destination MAC address pair', 'ISODOW', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

g_dom = []
g_dom.append(graph('record count', 'DAY', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_dom.append(graph('unique IP address', 'DAY', get_unique_ip_count, None, ('IPv4', 'IPv6',), None, graph.SumOrCount.none))
g_dow.append(graph('IP protocol counts', 'DAY', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_dom.append(graph('destination port', 'DAY', get_count_per, 'dst_port', ports, None, graph.SumOrCount.none))
g_dom.append(graph('source/destination IP address pair', 'DAY', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_dom.append(graph('source/destination MAC address pair', 'DAY', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

g_month = []
g_month.append(graph('record count', 'MONTH', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_month.append(graph('unique IP address', 'MONTH', get_unique_ip_count, None, ('IPv4', 'IPv6',), None, graph.SumOrCount.none))
g_month.append(graph('IP protocol counts', 'MONTH', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_month.append(graph('destination port', 'MONTH', get_count_per, 'dst_port', ports, None, graph.SumOrCount.none))
g_month.append(graph('source/destination IP address pair', 'MONTH', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_month.append(graph('source/destination MAC address pair', 'MONTH', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

def update(graphs):
    for g in graphs:
        g.update()

def create_byte_sum_sub_tabs():
    with ui.column().classes('w-full'):
        with ui.tabs().classes('w-full') as tabs_tb:
            tabs_tb_one = ui.tab('per hour')
            tabs_tb_two = ui.tab('per day-of-week')
            tabs_tb_three = ui.tab('per day of the month')
            tabs_tb_four = ui.tab('per month')

        with ui.tab_panels(tabs_tb, value=tabs_tb_one).classes('w-full'):
            with ui.tab_panel(tabs_tb_one):
                with ui.row():
                    for g in g_hour_b:
                        g.begin()
                ui.button('refresh', on_click=lambda: update(g_hour))

def create_record_count_sub_tabs():
    with ui.column().classes('w-full'):
        with ui.tabs().classes('w-full') as tabs_tc:
            tabs_tc_one = ui.tab('per hour')
            tabs_tc_two = ui.tab('per day-of-week')
            tabs_tc_three = ui.tab('per day of the month')
            tabs_tc_four = ui.tab('per month')

        with ui.tab_panels(tabs_tc, value=tabs_tc_one).classes('w-full'):
            with ui.tab_panel(tabs_tc_one):
                with ui.row():
                    for g in g_hour:
                        g.begin()
                ui.button('refresh', on_click=lambda: update(g_hour))

            with ui.tab_panel(tabs_tc_two):
                with ui.row():
                    for g in g_dow:
                        g.begin()
                ui.button('refresh', on_click=lambda: update(g_dow))

            with ui.tab_panel(tabs_tc_three):
                with ui.row():
                    for g in g_dom:
                        g.begin()
                ui.button('refresh', on_click=lambda: update(g_dom))

            with ui.tab_panel(tabs_tc_four):
                with ui.row():
                    for g in g_month:
                        g.begin()
                ui.button('refresh', on_click=lambda: update(g_month))

with ui.column().classes('w-full'):
    dark = ui.dark_mode()

    with ui.row():
        ui.label('switch mode:')
        ui.button('dark', on_click=dark.enable)
        ui.button('light', on_click=dark.disable)

    with ui.column().classes('w-full'):
        with ui.tabs().classes('w-full') as tabs_main:
                tabs_main_bytes = ui.tab('bytes')
                tabs_main_count = ui.tab('count')

        with ui.tab_panels(tabs_main, value=tabs_main_bytes).classes('w-full'):
            with ui.tab_panel(tabs_main_bytes):
                create_byte_sum_sub_tabs()

            with ui.tab_panel(tabs_main_count):
                create_record_count_sub_tabs()

ui.run(show=False)
