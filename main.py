#! /usr/bin/python3

import asyncio
from data import init_db
from fastapi import FastAPI
from nicegui import ui, app

app.on_startup(init_db)

from global_statistics import global_statistics
ui.link('global statistics', global_statistics)
ui.run(show=False)
