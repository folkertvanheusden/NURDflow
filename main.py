#! /usr/bin/python3

import copy
from data import get_record_count, get_unique_ip_count, get_count_per, get_count_per_src_dst_address, get_count_per_src_dst_mac_address
from nicegui import ui

class graph:
    def __init__(self, title, group_by, data_function, column, items, where):
        self.title = title
        self.group_by = group_by
        self.data_function = data_function
        self.column = column
        self.items = items
        self.where = where

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
            data = self.data_function(self.group_by, self.column, self.items, self.where)
            for item in data:
                if item[2] in item_names:
                    nr = item_names.index(item[2])
                else:
                    nr = -1
                self.fig['data'][nr]['x'].append(item[0])
                self.fig['data'][nr]['y'].append(item[1])
        else:
            data = self.data_function(self.group_by)
            for iy in range(len(self.items)):
                for item in data:
                    self.fig['data'][iy]['x'].append(item[0])
                    self.fig['data'][iy]['y'].append(item[1 + iy])
        self.plot.update()

ports = ((80, 'HTTP'), (443, 'HTTPS'), (5900, 'VNC'), (123, 'NTP'))
ip_protocols = ((1, 'ICMP'), (6, 'TCP'), (17, 'UDP'))

g_hour = []
g_hour.append(graph('record count', 'HOUR', get_record_count, None, (('n', 'n'),), None))
g_hour.append(graph('unique IP address', 'HOUR', get_unique_ip_count, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None))
g_hour.append(graph('IP protocol counts', 'HOUR', get_count_per, 'ip_protocol', ip_protocols, None))
g_hour.append(graph('destination port', 'HOUR', get_count_per, 'dst_port', ports, '(ip_protocol = 6 OR ip_protocol = 17)'))
g_hour.append(graph('source/destination IP address pair', 'HOUR', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None))
g_hour.append(graph('source/destination MAC address pair', 'HOUR', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None))

g_dow = []
g_dow.append(graph('record count', 'ISODOW', get_record_count, None, (('n', 'n'),), None))
g_dow.append(graph('unique IP address', 'ISODOW', get_unique_ip_count, None, ('IPv4', 'IPv6',), None))
g_dow.append(graph('IP protocol counts', 'ISODOW', get_count_per, 'ip_protocol', ip_protocols, None))
g_dow.append(graph('destination port', 'ISODOW', get_count_per, 'dst_port', ports, None))
g_dow.append(graph('source/destination IP address pair', 'ISODOW', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None))
g_dow.append(graph('source/destination MAC address pair', 'ISODOW', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None))

g_dom = []
g_dom.append(graph('record count', 'DAY', get_record_count, None, (('n', 'n'),), None))
g_dom.append(graph('unique IP address', 'DAY', get_unique_ip_count, None, ('IPv4', 'IPv6',), None))
g_dow.append(graph('IP protocol counts', 'DAY', get_count_per, 'ip_protocol', ip_protocols, None))
g_dom.append(graph('destination port', 'DAY', get_count_per, 'dst_port', ports, None))
g_dom.append(graph('source/destination IP address pair', 'DAY', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None))
g_dom.append(graph('source/destination MAC address pair', 'DAY', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None))

g_month = []
g_month.append(graph('record count', 'MONTH', get_record_count, None, (('n', 'n'),), None))
g_month.append(graph('unique IP address', 'MONTH', get_unique_ip_count, None, ('IPv4', 'IPv6',), None))
g_month.append(graph('IP protocol counts', 'MONTH', get_count_per, 'ip_protocol', ip_protocols, None))
g_month.append(graph('destination port', 'MONTH', get_count_per, 'dst_port', ports, None))
g_month.append(graph('source/destination IP address pair', 'MONTH', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None))
g_month.append(graph('source/destination MAC address pair', 'MONTH', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None))

def update(graphs):
    for g in graphs:
        g.update()

with ui.column().classes('w-full'):
    dark = ui.dark_mode()

    with ui.row():
        ui.label('Switch mode:')
        ui.button('Dark', on_click=dark.enable)
        ui.button('Light', on_click=dark.disable)

    with ui.tabs().classes('w-full') as tabs_t:
        tabs_t_one = ui.tab('per hour')
        tabs_t_two = ui.tab('per day-of-week')
        tabs_t_three = ui.tab('per day of the month')
        tabs_t_four = ui.tab('per month')

    with ui.tab_panels(tabs_t, value=tabs_t_one).classes('w-full'):
        with ui.tab_panel(tabs_t_one):
            with ui.row():
                for g in g_hour:
                    g.begin()
            ui.button('refresh', on_click=lambda: update(g_hour))

        with ui.tab_panel(tabs_t_two):
            with ui.row():
                for g in g_dow:
                    g.begin()
            ui.button('refresh', on_click=lambda: update(g_dow))

        with ui.tab_panel(tabs_t_three):
            with ui.row():
                for g in g_dom:
                    g.begin()
            ui.button('refresh', on_click=lambda: update(g_dom))

        with ui.tab_panel(tabs_t_four):
            with ui.row():
                for g in g_month:
                    g.begin()
            ui.button('refresh', on_click=lambda: update(g_month))

ui.run(show=False)
