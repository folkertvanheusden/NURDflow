#! /usr/bin/python3

import asyncio
from global_statistics import global_statistics
from nicegui import ui

ui.link('global statistics', global_statistics)

ui.run(show=False)
