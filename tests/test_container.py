import pytest

from container import TableContainer, Count, Sum, AggList, AggSet, FilteredCount

headers1 = ('name', 'age', 'gender')
table1 = [('toto', 12, 'M'), ('bob', 23, 'M'), ('bobette', 22, 'F')]


def test_empty():
    c = TableContainer([], ())
    assert c.idx == {}


def test_basic():
    c = TableContainer(table1, headers1)
    assert len(c) == 3
    assert c[0] is table1[0]


def test_distinct():
    c = TableContainer(table1, headers1)
    assert c.distinct(('gender',)) == {('F',), ('M',)}


def test_aggregate():
    c = TableContainer(table1, headers1)
    agg = c.aggregate(('gender',), (('age', Count), ('age', Sum)))
    assert agg[0][('M',)] == 2
    assert agg[0][('F',)] == 1
    assert agg[1][('M',)] == 35
    assert agg[1][('F',)] == 22


def test_aggregate_filter():
    c = TableContainer(table1, headers1)
    agg = c.aggregate(('gender',), (('age', Count), ('age', Sum)), (('gender', 'M'),))
    assert agg[0][('M',)] == 2
    with pytest.raises(KeyError):
        agg[0][('F',)]
    assert agg[1][('M',)] == 35
    with pytest.raises(KeyError):
        agg[1][('F',)]


headers2 = ('equipment', 'chipset', 'port', 'operator', 'status')
table2 = [
    ('equip1', 'chip1', 0, 'op1', 'used'),
    ('equip1', 'chip1', 1, 'op1', 'used'),
    ('equip1', 'chip1', 2, 'op1', 'used'),
    ('equip1', 'chip1', 3, 'op1', 'used'),
    ('equip1', 'chip1', 4, None, 'free'),
    ('equip1', 'chip1', 5, None, 'free'),
    ('equip1', 'chip2', 0, 'op2', 'used'),
    ('equip1', 'chip2', 1, 'op2', 'used'),
    ('equip1', 'chip2', 2, 'op2', 'used'),
    ('equip1', 'chip2', 3, None, 'free'),
]


def test_filter():
    c = TableContainer(table2, headers2)
    assert len(list(c.filter(('status', 'free')))) == 3
    assert len(list(c.filter(('status', 'used'), ('operator', 'op1')))) == 4


def test_distinct_not_none():
    c = TableContainer(table2, headers2)
    assert c.distinct(('operator',), (('operator__not', None),)) == {('op1',), ('op2',)}


def test_aggregate_list_set():
    c = TableContainer(table2, headers2)
    assert c.aggregate(('equipment',), (('operator', AggList),), filters=(('operator__not', None),))[0] == \
           {('equip1',): ['op1', 'op1', 'op1', 'op1', 'op2', 'op2', 'op2']}
    assert c.aggregate(('equipment',), (('operator', AggSet),), filters=(('operator__not', None),))[0] == \
           {('equip1',): {'op1', 'op2'}}

table3 = [
    ('equip1', 'chip1', 0, 'op1', 'used'),
    ('equip1', 'chip1', 1, 'op1', 'preempted'),
    ('equip1', 'chip1', 2, None, 'free'),
    ('equip1', 'chip1', 3, None, 'free'),
    ('equip2', 'chip2', 1, None, 'free'),
    ('equip2', 'chip2', 2, None, 'free'),
]


def test_aggregate_count():
    c = TableContainer(table3, headers2)
    assert c.aggregate(('equipment',), (('status', Count),), filters=(('status__not', 'free'),))[0] == \
           {('equip1',): 2}
    assert c.aggregate(('equipment',), (('status', FilteredCount('not', 'free')),))[0] == \
           {('equip1',): 2, ('equip2',): 0}
