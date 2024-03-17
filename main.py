#! /usr/bin/python3

import copy
from data import *
from nicegui import ui

def create_fig_card(data, title, ip):
    fig_template = {
        'data': [
            {
                'type': 'scatter',
                'name': '',
                'x': [],
                'y': [],
            }
        ],
        'layout': {
            'margin': {'l': 30, 'r': 0, 't': 0, 'b': 15},
            'plot_bgcolor': '#E5ECF6',
            'xaxis': {'gridcolor': 'white'},
            'yaxis': {'gridcolor': 'white'},
        },
    }

    fig = copy.deepcopy(fig_template)

    if ip:
        fig['data'][0]['name'] = 'IPv4'
        fig['data'].append({
                'type': 'scatter',
                'name': 'IPv6',
                'x': [],
                'y': [],
            })

    for item in data:
        fig['data'][0]['x'].append(item[0])
        fig['data'][0]['y'].append(item[1])
        if ip:
            fig['data'][1]['x'].append(item[0])
            fig['data'][1]['y'].append(item[2])

    with ui.card():
        ui.label(title)
        ui.plotly(fig).classes('w-100 h-80')

with ui.tabs().classes('w-full') as tabs_t:
    tabs_t_one = ui.tab('per hour')
    tabs_t_two = ui.tab('per day-of-week')
    tabs_t_three = ui.tab('per month')

with ui.tab_panels(tabs_t, value=tabs_t_one).classes('w-full'):
    with ui.tab_panel(tabs_t_one):
        with ui.row():
            create_fig_card(get_record_count_per_hour(), 'records per hour', False)
            create_fig_card(get_unique_ip_count_per_hour(), 'unique IP4/6 addresses per hour', True)

    with ui.tab_panel(tabs_t_two):
        with ui.row():
            create_fig_card(get_record_count_per_day_of_week(), 'records per day-of-week', False)
            create_fig_card(get_unique_ip_count_per_day_of_week(), 'unique IP4/6 addresses per day-of-week', True)

    with ui.tab_panel(tabs_t_three):
        with ui.row():
            create_fig_card(get_record_count_per_month(), 'records per month', False)
            create_fig_card(get_unique_ip_count_per_month(), 'unique IP4/6 addr per month', True)

ui.run(show=False)
