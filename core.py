
import pprint

from dbdriver import ddb, DynamoDbDriver
from type_registry import _tr
from timestamp import Timestamp
from db import ObjectDb, UnionDb
from encdec import EncDec
from noval import _noVal
from context import Context
from uuid import getUUID
from objmeta import DBOMeta, EntityMeta, EventMeta
from obj import Entity, Event
from utils import displayTable
from graph import node
from root_clock import RootClock
from clock import Clock, ClockEvent
from dictutils import merge, flatten

import refdata

rawdb = DynamoDbDriver(ddb)
_odb = ObjectDb(rawdb)
RootClock('Main', db=_odb).write()

import demos.refdata

demos.refdata.main(rawdb, _odb)

import workflow

import demos.workflow

demos.workflow.main(_odb)
