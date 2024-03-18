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

def create_fig_card_per(data, title, items):
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

    fig = copy.deepcopy(fig_template)

    for item in items:
        fig['data'].append({
                'type': 'scatter',
                'name': item,
                'x': [],
                'y': [],
            })

    for item in data:
        nr = items.index(item[2])
        fig['data'][nr]['x'].append(item[0])
        fig['data'][nr]['y'].append(item[1])

    with ui.card():
        ui.label(title)
        ui.plotly(fig).classes('w-100 h-80')

with ui.column():
    dark = ui.dark_mode()

    with ui.row():
        ui.label('Switch mode:')
        ui.button('Dark', on_click=dark.enable)
        ui.button('Light', on_click=dark.disable)

    with ui.tabs().classes('w-full') as tabs_t:
        tabs_t_one = ui.tab('per hour')
        tabs_t_two = ui.tab('per day-of-week')
        tabs_t_three = ui.tab('per day-of-the-month')
        tabs_t_four = ui.tab('per month')

    with ui.tab_panels(tabs_t, value=tabs_t_one).classes('w-full'):
        ports = (80, 443, 5900, 123)
        with ui.tab_panel(tabs_t_one):
            with ui.row():
                create_fig_card(get_record_count_per_hour(), 'records', False)
                create_fig_card(get_unique_ip_count_per_hour(), 'unique IP4/6 addresses', True)
                create_fig_card_per(get_count_per_port_per_hour(ports), 'count per dst-port', ports)
                create_fig_card(get_count_per_src_dst_address_per_hour(), 'source/destination IP address pairs', True)
                create_fig_card(get_count_per_src_dst_mac_address_per_hour(), 'source/destination MAC address pairs', False)

        with ui.tab_panel(tabs_t_two):
            with ui.row():
                create_fig_card(get_record_count_per_day_of_week(), 'records', False)
                create_fig_card(get_unique_ip_count_per_day_of_week(), 'unique IP4/6 addresses', True)
                create_fig_card_per(get_count_per_port_per_day_of_week(ports), 'count per dst-port', ports)
                create_fig_card(get_count_per_src_dst_address_per_day_of_week(), 'source/destination IP address pairs', True)
                create_fig_card(get_count_per_src_dst_mac_address_per_day_of_week(), 'source/destination MAC address pairs', False)

        with ui.tab_panel(tabs_t_three):
            with ui.row():
                create_fig_card(get_record_count_per_month_day(), 'records', False)
                create_fig_card(get_unique_ip_count_per_month_day(), 'unique IP4/6 addresses', True)
                create_fig_card_per(get_count_per_port_per_month_day(ports), 'count per dst-port', ports)
                create_fig_card(get_count_per_src_dst_address_per_month_day(), 'source/destination IP address pairs', True)
                create_fig_card(get_count_per_src_dst_mac_address_per_month_day(), 'source/destination MAC address pairs', False)

        with ui.tab_panel(tabs_t_four):
            with ui.row():
                create_fig_card(get_record_count_per_month(), 'records', False)
                create_fig_card(get_unique_ip_count_per_month(), 'unique IP4/6 addr', True)
                create_fig_card_per(get_count_per_port_per_month(ports), 'count per dst-port', ports)
                create_fig_card(get_count_per_src_dst_address_per_month(), 'source/destination IP address pairs', True)
                create_fig_card(get_count_per_src_dst_mac_address_per_month(), 'source/destination MAC address pairs', False)

ui.run(show=False)
