#! /usr/bin/python3

import copy
from data import *
from nicegui import ui

with ui.tabs().classes('w-full') as tabs_t:
    tabs_t_one = ui.tab('per hour')
    tabs_t_two = ui.tab('per day-of-week')
    tabs_t_three = ui.tab('per month')

with ui.tab_panels(tabs_t, value=tabs_t_one).classes('w-full'):
    fig_template = {
        'data': [
            {
                'type': 'scatter',
                'name': '',
                'x': [],
                'y': [],
            },
        ],
        'layout': {
            'margin': {'l': 30, 'r': 0, 't': 0, 'b': 15},
            'plot_bgcolor': '#E5ECF6',
            'xaxis': {'gridcolor': 'white'},
            'yaxis': {'gridcolor': 'white'},
        },
    }

    with ui.tab_panel(tabs_t_one):
        records_per_hour = get_record_count_per_hour()
        fig = copy.deepcopy(fig_template)
        fig['data'][0]['name'] = 'records per hour'
        for item in records_per_hour:
            fig['data'][0]['x'].append(item[0])
            fig['data'][0]['y'].append(item[1])
        ui.plotly(fig).classes('w-100 h-80')

    with ui.tab_panel(tabs_t_two):
        records_per_hour = get_record_count_per_day_of_week()
        fig = copy.deepcopy(fig_template)
        fig['data'][0]['name'] = 'records per day-of-week'
        for item in records_per_hour:
            fig['data'][0]['x'].append(item[0])
            fig['data'][0]['y'].append(item[1])
        ui.plotly(fig).classes('w-100 h-80')

    with ui.tab_panel(tabs_t_three):
        records_per_month = get_record_count_per_month()
        fig = copy.deepcopy(fig_template)
        fig['data'][0]['name'] = 'records per month'
        for item in records_per_month:
            fig['data'][0]['x'].append(item[0])
            fig['data'][0]['y'].append(item[1])
        ui.plotly(fig).classes('w-100 h-80')

ui.run(show=False)
