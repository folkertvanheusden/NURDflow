#! /usr/bin/python3

import asyncio
import asyncpg
import copy
from data import *
from enum import Enum
from nicegui import ui
import time

class as_is_plot:
    class SumOrCount(Enum):
        bcount = True
        ncount = False
        none   = False

    def __init__(self, title, sum_bytes: SumOrCount):
        self.title = title
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

        self.fig['data'].append({
                'type': 'scatter',
                'name': title,
                'x': [],
                'y': [],
            })

    async def begin(self):
        with ui.card().classes('w-100 h-80'):
            ui.label(self.title)
            plot = ui.plotly(self.fig).classes('w-100 h-80')
        self.plot = plot

    async def refresh_data(self, data):
        self.fig['data'][0]['x'].clear()
        self.fig['data'][0]['y'].clear()
        for item in data:
            self.fig['data'][0]['x'].append(item['ts'])
            self.fig['data'][0]['y'].append(item['sum'] if self.sum_bytes == line_plot.SumOrCount.bcount else item['count'])

    @ui.refreshable
    async def update(self):
        if self.plot:
            self.plot.update()


class line_plot:
    class SumOrCount(Enum):
        bcount = True
        ncount = False
        none   = False

    def __init__(self, title, group_by, sum_bytes: SumOrCount):
        self.title = title
        self.group_by = group_by
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

        self.fig['data'].append({
                'type': 'scatter',
                'name': title,
                'x': [],
                'y': [],
            })

    async def begin(self):
        with ui.card().classes('w-100 h-80'):
            ui.label(self.title)
            plot = ui.plotly(self.fig).classes('w-100 h-80')

        self.plot = plot

    async def refresh_data(self, data):
        self.fig['data'][0]['x'].clear()
        self.fig['data'][0]['y'].clear()
        for item in data:
            self.fig['data'][0]['x'].append(item['_id'])
            self.fig['data'][0]['y'].append(item['sum'] if self.sum_bytes == line_plot.SumOrCount.bcount else item['count'])

    @ui.refreshable
    async def update(self):
        if self.plot:
            self.plot.update()


class lines_plot:
    class SumOrCount(Enum):
        bcount = True
        ncount = False
        none   = False

    def __init__(self, title, group_by, field, field_values, sum_bytes: SumOrCount):
        print(title)
        self.title = title
        self.group_by = group_by
        self.field = field
        self.field_values = copy.deepcopy(list(field_values))
        self.sum_bytes = sum_bytes
        self.plot = None

        self.field_values.append(('other', 'other'))

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

        for i in self.field_values:
            self.fig['data'].append({
                    'type': 'scatter',
                    'name': i[0],
                    'x': [],
                    'y': [],
                })

    async def begin(self):
        with ui.card().classes('w-100 h-80'):
            ui.label(self.title)
            plot = ui.plotly(self.fig).classes('w-100 h-80')
        self.plot = plot

    async def refresh_data(self, data):
        for i in range(len(self.fig['data'])):
            self.fig['data'][i]['x'].clear()
            self.fig['data'][i]['y'].clear()
        for field_index in range(len(self.field_values)):
            current_field = self.field_values[field_index][0]
            array_index = [x for x, y in enumerate(self.field_values) if y[0] == current_field][0]
            for item in data:
                if item['_id']['field'] == current_field:
                    self.fig['data'][array_index]['x'].append(item['_id']['ts_component'])
                    self.fig['data'][array_index]['y'].append(item['sum'] if self.sum_bytes == line_plot.SumOrCount.bcount else item['count'])

    @ui.refreshable
    async def update(self):
        if self.plot:
            self.plot.update()


class heatmap:
    class SumOrCount(Enum):
        bcount = True
        ncount = False
        none   = False

    def __init__(self, width, group_by_x, height, group_by_y, sum_or_count=SumOrCount.none):
        self.height = height
        self.group_by_y = group_by_y
        self.width = width
        self.group_by_x = group_by_x
        self.sum_or_count = sum_or_count

    async def begin(self):
        with ui.card():
            ui.label('total number of bytes transferred' if self.sum_or_count == heatmap.SumOrCount.bcount else 'total number of packets transferred')
            g = ui.grid(columns=1)
        self.grid = g

    async def refresh_data(self, data):
        # {'_id': {'field_1': 6, 'field_2': 3}, 'sum': 36580947, 'count': 2462}

        highest = 0
        if self.sum_or_count == heatmap.SumOrCount.bcount:
            for element in data:
                highest = max(highest, element['sum'])
        else:
            for element in data:
                highest = max(highest, element['count'])

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
            p = (element['sum'] if self.sum_or_count == heatmap.SumOrCount.bcount else element['count']) / highest
            r = 255 * p
            g = 255 - r
            b = 0
            x = element['_id']['field_1']
            y = element['_id']['field_2'] - 1
            svg += f'<rect width="{x_mul}" height="{y_mul}" x="{int(x * x_mul) + t_w * 1.5}" y="{int(y * y_mul) + t_h * 1.5}" fill="#{int(r):02x}{int(g):02x}{int(b):02x}" />\n'
        svg += '</svg>'

        self.svg = svg

    @ui.refreshable
    async def update(self):
        with self.grid:
            self.grid.clear()
            ui.html(self.svg)

ports = ((80, 'HTTP'), (443, 'HTTPS'), (22, 'SSH'))
ip_protocols = ((1, 'ICMP'), (6, 'TCP'), (17, 'UDP'))

objects_heatmaps = []
objects_heatmaps.append({ 'graph-bytes': heatmap(24, 'hour', 7, 'isoDayOfWeek', heatmap.SumOrCount.bcount),
                          'graph-count': heatmap(24, 'hour', 7, 'isoDayOfWeek', heatmap.SumOrCount.ncount),
                          'data': lambda: get_heatmap('hour', 'isoDayOfWeek'),
                          'title': 'when are the most sessions' })

objects_per_as_is = []
objects_per_as_is.append({ 'graph-bytes': as_is_plot('as-is bytes', line_plot.SumOrCount.bcount),
                           'graph-count': as_is_plot('as-is packet count', line_plot.SumOrCount.ncount),
                           'data': lambda: get_field_grouped('data.octetDeltaCount'),
                           'title': 'on-going data' })

objects_per_hour = []
objects_per_hour.append({ 'graph-bytes': line_plot('bytes per hour', 'hour', line_plot.SumOrCount.bcount),
                          'graph-count': line_plot('count per hour', 'hour', line_plot.SumOrCount.ncount),
                          'data': lambda: get_record('hour'),
                          'title': 'counts aggregated per hour'
                         })
objects_per_hour.append({ 'graph-bytes': lines_plot('bytes per hour', 'hour', 'data.destinationTransportPort', ports, line_plot.SumOrCount.bcount),
                          'graph-count': lines_plot('count per hour', 'hour', 'data.destinationTransportPort', ports, line_plot.SumOrCount.ncount),
                          'data': lambda: get_COUNT_PER('data.destinationTransportPort', ports, 'hour'),
                          'title': 'counts aggregated per port'
                         })

objects_per_day_of_week = []
objects_per_day_of_week.append({ 'graph-bytes': line_plot('bytes per day-of-week', 'isoDayOfWeek', line_plot.SumOrCount.bcount),
                                 'graph-count': line_plot('count per day-of-week', 'isoDayOfWeek', line_plot.SumOrCount.ncount),
                                 'data': lambda: get_record('isoDayOfWeek'),
                                 'title': 'counts aggregated per hour'
                                })
objects_per_day_of_week.append({ 'graph-bytes': lines_plot('bytes per day-of-week', 'isoDayOfWeek', 'data.destinationTransportPort', ports, line_plot.SumOrCount.bcount),
                          'graph-count': lines_plot('count per day-of-week', 'isoDayOfWeek', 'data.destinationTransportPort', ports, line_plot.SumOrCount.ncount),
                          'data': lambda: get_COUNT_PER('data.destinationTransportPort', ports, 'isoDayOfWeek'),
                          'title': 'counts aggregated per port'
                         })

objects_per_month = []
objects_per_month.append({ 'graph-bytes': line_plot('bytes per month', 'month', line_plot.SumOrCount.bcount),
                           'graph-count': line_plot('count per month', 'month', line_plot.SumOrCount.ncount),
                           'data': lambda: get_record('month'),
                           'title': 'counts aggregated per hour'
                         })
objects_per_month.append({ 'graph-bytes': lines_plot('bytes per month', 'month', 'data.destinationTransportPort', ports, line_plot.SumOrCount.bcount),
                          'graph-count': lines_plot('count per month', 'month', 'data.destinationTransportPort', ports, line_plot.SumOrCount.ncount),
                          'data': lambda: get_COUNT_PER('data.destinationTransportPort', ports, 'month'),
                          'title': 'counts aggregated per port'
                         })

async def update(objects):
    print('update')
    for obj in objects:
        data = await obj['data']()
        await obj['graph-bytes'].refresh_data(data)
        await obj['graph-count'].refresh_data(data)
        await obj['graph-bytes'].update()
        await obj['graph-count'].update()

async def create(objects):
    print('create')
    for obj in objects:
        data = await obj['data']()
        ui.label(obj['title']).classes('text-h5')
        with ui.row():
            await obj['graph-bytes'].begin()
            await obj['graph-count'].begin()
    ui.button('refresh', on_click=lambda: update(objects))

@ui.page('/global-statistics', response_timeout=60)
async def global_statistics():
    tstart = time.time()
    with ui.header(elevated=True).style('background-color: #3874c8').classes('items-center justify-between'):
        with ui.row():
            ui.label('NURDflow - ' + get_collection()).style('font-size: max(10pt, 3vh)')

    with ui.footer().style('background-color: #3874c8'):
        with ui.row():
            dark = ui.dark_mode()
            ui.label('switch mode:').style('font-size: max(8pt, 2vh)')
            ui.button('dark', on_click=dark.enable)
            ui.button('light', on_click=dark.disable)

    with ui.column().classes('w-full'):
        with ui.column().classes('w-full'):
            with ui.tabs().classes('w-full') as tabs_main:
                tabs_main_heatmaps = ui.tab('heatmaps')
                tabs_main_as_is = ui.tab('as-is')
                tabs_main_hour = ui.tab('per hour')
                tabs_main_day_of_week = ui.tab('per day of week')
                tabs_main_month = ui.tab('per month')

            with ui.tab_panels(tabs_main, value=tabs_main_heatmaps).classes('w-full'):
                with ui.tab_panel(tabs_main_heatmaps):
                    await create(objects_heatmaps)
                    await update(objects_heatmaps)

                with ui.tab_panel(tabs_main_as_is):
                    await create(objects_per_as_is)
                    await update(objects_per_as_is)

                with ui.tab_panel(tabs_main_hour):
                    await create(objects_per_hour)
                    await update(objects_per_hour)

                with ui.tab_panel(tabs_main_day_of_week):
                    await create(objects_per_day_of_week)
                    await update(objects_per_day_of_week)

                with ui.tab_panel(tabs_main_month):
                    await create(objects_per_month)
                    await update(objects_per_month)

    print('TOOK', time.time() - tstart)
