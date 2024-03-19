#! /usr/bin/python3

import asyncio
import copy
from data import get_record, get_unique_ip_count, get_count_per, get_count_per_src_dst_address, get_count_per_src_dst_mac_address, get_heatmap_data, get_flow_duration_groups
from enum import Enum
from nicegui import ui
import time

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
        self.plot = None

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

    async def begin(self):
        with ui.card().classes('w-100 h-80'):
            ui.label(self.title)
            plot = ui.plotly(self.fig).classes('w-100 h-80')

        self.plot = plot

    async def refresh_data(self):
        item_names = [v[0] for v in self.items]
        for iy in range(len(self.items)):
            self.fig['data'][iy]['x'].clear()
            self.fig['data'][iy]['y'].clear()
        if self.column:
            self.fig['data'][-1]['x'].clear()
            self.fig['data'][-1]['y'].clear()
            if self.sum_bytes == graph.SumOrCount.bcount:
                data = await self.data_function(self.group_by, self.column, self.items, self.where, True)
            elif self.sum_bytes == graph.SumOrCount.ncount:
                data = await self.data_function(self.group_by, self.column, self.items, self.where, False)
            for item in data:
                if item[2] in item_names:
                    nr = item_names.index(item[2])
                else:
                    nr = -1
                self.fig['data'][nr]['x'].append(item[0])
                self.fig['data'][nr]['y'].append(item[1])
        else:
            if self.sum_bytes == graph.SumOrCount.bcount:
                data = await self.data_function(self.group_by, True)
            elif self.sum_bytes == graph.SumOrCount.ncount:
                data = await self.data_function(self.group_by, False)
            for iy in range(len(self.items)):
                for item in data:
                    self.fig['data'][iy]['x'].append(item[0])
                    self.fig['data'][iy]['y'].append(item[1 + iy])

    @ui.refreshable
    async def update(self):
        if self.plot:
            self.plot.update()

class heatmap:
    class SumOrCount(Enum):
        bcount = True
        ncount = False
        none   = False

    def __init__(self, height, group_by_y, width, group_by_x, sum_bytes: SumOrCount):
        self.height = height
        self.group_by_y = group_by_y
        self.width = width
        self.group_by_x = group_by_x
        self.sum_bytes = sum_bytes

    async def begin(self):
        with ui.card():
            g = ui.grid(columns=1)
        self.grid = g

    async def refresh_data(self):
        data = await get_heatmap_data(self.group_by_y, self.group_by_x, self.sum_bytes)

        highest = 0
        for element in data:
            highest = max(highest, element[2])

        x_mul = 25  # rectangles
        y_mul = 25
        t_w   = 15  # text
        t_h   = 15

        svg = f'<svg viewBox="0 0 {self.width * x_mul + t_w * 1.5} {self.height * y_mul + t_h * 1.5}" width="{self.width * x_mul}" height="{self.height * y_mul}" xmlns="http://www.w3.org/2000/svg">\n'
        svg += '<style>.small { font: italic ' + f'{t_h}px' + ' serif; fill: black; }</style>'

        for x in range(self.width):
            svg += f'<text x="{x * x_mul + t_w * (1.5 + 0.5)}" y="{t_h}" class="small">{x}</text>'

        for y in range(self.height):
            svg += f'<text x="{t_w // 2}" y="{y * y_mul + t_h * (1.5 + 1.0)}" class="small">{y + 1}</text>'

        for element in data:
            p = element[2] / highest
            r = 255 * p
            g = 255 - r
            b = 0
            svg += f'<rect width="{x_mul}" height="{y_mul}" x="{int(element[1] * x_mul) + t_w * 1.5}" y="{int(element[0] * y_mul) + t_h * 1.5}" fill="#{int(r):02x}{int(g):02x}{int(b):02x}" />\n'
        svg += '</svg>'

        self.svg = svg

    @ui.refreshable
    async def update(self):
        with self.grid:
            self.grid.clear()
            ui.html(self.svg)

class bar_chart:
    def __init__(self, title, data_getter):
        self.data_getter = data_getter

        fig_template = {
            'title': title,
            'chart': {'type': 'column'},
            'xAxis': {'categories': []},
            'yAxis': {'name': 'count'},
            'series': [
                {'name': 'duration-count', 'data': []},
            ],
        }

        self.fig = copy.deepcopy(fig_template)

    async def begin(self):
        with ui.card():
            self.plot = ui.highchart(self.fig).classes('w-150 h-150')

    async def refresh_data(self):
        data = await self.data_getter(40)

        self.fig['xAxis']['categories'].clear()
        for item in data:
            self.fig['xAxis']['categories'].append(item[0])

        self.fig['series'][0]['data'].clear()
        for item in data:
            self.fig['series'][0]['data'].append(item[1])

    @ui.refreshable
    async def update(self):
        self.plot.update()

ports = ((80, 'HTTP'), (443, 'HTTPS'), (5900, 'VNC'), (123, 'NTP'))
ip_versions = ((4, 'IPv4'), (6, 'IPv6'))
ip_protocols = ((1, 'ICMP'), (6, 'TCP'), (17, 'UDP'))

g_hour_c = []
g_hour_c.append(graph('record count', 'HOUR', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_hour_c.append(graph('unique IP address', 'HOUR', get_unique_ip_count, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_hour_c.append(graph('IP version', 'HOUR', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.none))
g_hour_c.append(graph('IP protocol counts', 'HOUR', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_hour_c.append(graph('destination port', 'HOUR', get_count_per, 'dst_port', ports, '(ip_protocol = 6 OR ip_protocol = 17)', graph.SumOrCount.none))
g_hour_c.append(graph('source/destination IP address pair', 'HOUR', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_hour_c.append(graph('source/destination MAC address pair', 'HOUR', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

g_hour_b = []
g_hour_b.append(graph('record byte sum', 'HOUR', get_record, None, (('n', 'n'),), None, graph.SumOrCount.bcount))
g_hour_b.append(graph('IP version', 'HOUR', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.bcount))
g_hour_b.append(graph('IP protocol counts', 'HOUR', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.bcount))
g_hour_b.append(graph('destination port', 'HOUR', get_count_per, 'dst_port', ports, '(ip_protocol = 6 OR ip_protocol = 17)', graph.SumOrCount.bcount))

g_dow_c = []
g_dow_c.append(graph('record count', 'ISODOW', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_dow_c.append(graph('unique IP address', 'ISODOW', get_unique_ip_count, None, ('IPv4', 'IPv6',), None, graph.SumOrCount.none))
g_dow_c.append(graph('IP version', 'ISODOW', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.none))
g_dow_c.append(graph('IP protocol counts', 'ISODOW', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_dow_c.append(graph('destination port', 'ISODOW', get_count_per, 'dst_port', ports, None, graph.SumOrCount.none))
g_dow_c.append(graph('source/destination IP address pair', 'ISODOW', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_dow_c.append(graph('source/destination MAC address pair', 'ISODOW', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

g_dow_b = []
g_dow_b.append(graph('record byte sum', 'ISODOW', get_record, None, (('n', 'n'),), None, graph.SumOrCount.bcount))
g_dow_b.append(graph('IP version', 'ISODOW', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.bcount))
g_dow_b.append(graph('IP protocol counts', 'ISODOW', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.bcount))
g_dow_b.append(graph('destination port', 'ISODOW', get_count_per, 'dst_port', ports, '(ip_protocol = 6 OR ip_protocol = 17)', graph.SumOrCount.bcount))

g_dom_c = []
g_dom_c.append(graph('record count', 'DAY', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_dom_c.append(graph('unique IP address', 'DAY', get_unique_ip_count, None, ('IPv4', 'IPv6',), None, graph.SumOrCount.none))
g_dom_c.append(graph('IP version', 'DAY', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.none))
g_dom_c.append(graph('IP protocol counts', 'DAY', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_dom_c.append(graph('destination port', 'DAY', get_count_per, 'dst_port', ports, None, graph.SumOrCount.none))
g_dom_c.append(graph('source/destination IP address pair', 'DAY', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_dom_c.append(graph('source/destination MAC address pair', 'DAY', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

g_dom_b = []
g_dom_b.append(graph('record byte sum', 'DAY', get_record, None, (('n', 'n'),), None, graph.SumOrCount.bcount))
g_dom_b.append(graph('IP version', 'DAY', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.bcount))
g_dom_b.append(graph('IP protocol counts', 'DAY', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.bcount))
g_dom_b.append(graph('destination port', 'DAY', get_count_per, 'dst_port', ports, '(ip_protocol = 6 OR ip_protocol = 17)', graph.SumOrCount.bcount))

g_month_c = []
g_month_c.append(graph('record count', 'MONTH', get_record, None, (('n', 'n'),), None, graph.SumOrCount.none))
g_month_c.append(graph('unique IP address', 'MONTH', get_unique_ip_count, None, ('IPv4', 'IPv6',), None, graph.SumOrCount.none))
g_month_c.append(graph('IP version', 'MONTH', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.none))
g_month_c.append(graph('IP protocol counts', 'MONTH', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.none))
g_month_c.append(graph('destination port', 'MONTH', get_count_per, 'dst_port', ports, None, graph.SumOrCount.none))
g_month_c.append(graph('source/destination IP address pair', 'MONTH', get_count_per_src_dst_address, None, (('IP4', 'IPv4'), ('IP6', 'IPv6')), None, graph.SumOrCount.none))
g_month_c.append(graph('source/destination MAC address pair', 'MONTH', get_count_per_src_dst_mac_address, None, (('n', 'n'),), None, graph.SumOrCount.none))

g_month_b = []
g_month_b.append(graph('record byte sum', 'MONTH', get_record, None, (('n', 'n'),), None, graph.SumOrCount.bcount))
g_month_b.append(graph('IP version', 'MONTH', get_count_per, 'ip_version', ip_versions, None, graph.SumOrCount.bcount))
g_month_b.append(graph('IP protocol counts', 'MONTH', get_count_per, 'ip_protocol', ip_protocols, None, graph.SumOrCount.bcount))
g_month_b.append(graph('destination port', 'MONTH', get_count_per, 'dst_port', ports, '(ip_protocol = 6 OR ip_protocol = 17)', graph.SumOrCount.bcount))

g_heatmap_b = []
g_heatmap_b.append(heatmap(7, 'ISODOW', 24, 'HOUR', graph.SumOrCount.bcount))

g_heatmap_c = []
g_heatmap_c.append(heatmap(7, 'ISODOW', 24, 'HOUR', graph.SumOrCount.ncount))

async def update(objs):
    tasks = []
    for o in objs:
        tasks.append(asyncio.create_task(o.refresh_data()))
    await asyncio.gather(*tasks)
    for o in objs:
        await o.update()

async def create_byte_sum_sub_tabs():
    with ui.column().classes('w-full'):
        with ui.tabs().classes('w-full') as tabs_tb:
            tabs_tb_one = ui.tab('per hour')
            tabs_tb_two = ui.tab('per day-of-week')
            tabs_tb_three = ui.tab('per day of the month')
            tabs_tb_four = ui.tab('per month')
            tabs_tb_five = ui.tab('heatmap')
 
        objs = []

        with ui.tab_panels(tabs_tb, value=tabs_tb_one).classes('w-full'):
            with ui.tab_panel(tabs_tb_one):
                with ui.row():
                    for g in g_hour_b:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_hour_b))

            with ui.tab_panel(tabs_tb_two):
                with ui.row():
                    for g in g_dow_b:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_dow_b))

            with ui.tab_panel(tabs_tb_three):
                with ui.row():
                    for g in g_dom_b:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_dom_b))

            with ui.tab_panel(tabs_tb_four):
                with ui.row():
                    for g in g_month_b:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_month_b))

            with ui.tab_panel(tabs_tb_five):
                with ui.row():
                    for g in g_heatmap_b:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_heatmap_b))

            tasks = []
            for o in objs:
                tasks.append(asyncio.create_task(o.refresh_data()))
            await asyncio.gather(*tasks)

            for o in objs:
                await o.update()

async def create_record_count_sub_tabs():
    with ui.column().classes('w-full'):
        with ui.tabs().classes('w-full') as tabs_tc:
            tabs_tc_one = ui.tab('per hour')
            tabs_tc_two = ui.tab('per day-of-week')
            tabs_tc_three = ui.tab('per day of the month')
            tabs_tc_four = ui.tab('per month')
            tabs_tc_five = ui.tab('heatmap')

        objs = []

        with ui.tab_panels(tabs_tc, value=tabs_tc_one).classes('w-full'):
            with ui.tab_panel(tabs_tc_one):
                with ui.row():
                    for g in g_hour_c:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_hour))

            with ui.tab_panel(tabs_tc_two):
                with ui.row():
                    for g in g_dow_c:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_dow))

            with ui.tab_panel(tabs_tc_three):
                with ui.row():
                    for g in g_dom_c:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_dom))

            with ui.tab_panel(tabs_tc_four):
                with ui.row():
                    for g in g_month_c:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_month))

            with ui.tab_panel(tabs_tc_five):
                with ui.row():
                    for g in g_heatmap_c:
                        await g.begin()
                        objs.append(g)
                ui.button('refresh', on_click=lambda: update(g_heatmap_c))

            tasks = []
            for o in objs:
                tasks.append(asyncio.create_task(o.refresh_data()))
            await asyncio.gather(*tasks)

            for o in objs:
                await o.update()

g_misc = []
g_misc.append(bar_chart('number of records per flow-duration interval', get_flow_duration_groups))

@ui.page('/global-statistics', response_timeout=60)
async def global_statistics():
    tstart = time.time()
    with ui.header(elevated=True).style('background-color: #3874c8').classes('items-center justify-between'):
        with ui.row():
            ui.label('NURDflow').style('font-size: max(10pt, 3vh)')

    with ui.footer().style('background-color: #3874c8'):
        with ui.row():
            dark = ui.dark_mode()
            ui.label('switch mode:').style('font-size: max(8pt, 2vh)')
            ui.button('dark', on_click=dark.enable)
            ui.button('light', on_click=dark.disable)

    with ui.column().classes('w-full'):
        with ui.column().classes('w-full'):
            with ui.tabs().classes('w-full') as tabs_main:
                    tabs_main_bytes = ui.tab('bytes')
                    tabs_main_count = ui.tab('count')
                    tabs_main_misc = ui.tab('miscellaneous')

            with ui.tab_panels(tabs_main, value=tabs_main_bytes).classes('w-full'):
                with ui.tab_panel(tabs_main_bytes):
                    await create_byte_sum_sub_tabs()

                with ui.tab_panel(tabs_main_count):
                    await create_record_count_sub_tabs()

                with ui.tab_panel(tabs_main_misc):
                    with ui.row():
                        for g in g_misc:
                            await g.begin()  # TODO obj
                    ui.button('refresh', on_click=lambda: update(g_misc))

    print('TOOK', time.time() - tstart)
