
import operator as ope


try:
    basestring
except NameError:
    basestring = str


def in_(a, b):
    return ope.contains(b, a)


class Filter:
    tr_python = {'not': ope.ne, 'in': in_, 'contains': ope.contains,
                 'gt': ope.gt, 'lt': ope.lt, 'gte': ope.ge, 'lte': ope.le, 'eq': ope.eq}
    tr_sql = {'not': '<>', 'in': ' IN ', 'like': ' LIKE ', 'gt': '>', 'lt': '<', 'gte': '>=', 'lte': '<=', 'eq': '='}

    @classmethod
    def translate_py(cls, *args):
        """ translate (key, value) filter to (key, op, value) filter where op is python operator
        :param args: (tuple) (key, value) or (key, op, value)
        :return: (tuple) (key, op, value)
        """
        # TODO add regex
        if len(args) is 3:
            # if (key, op, value), return unchanged
            return args
        key, value = args
        if '__' in key:
            key, op = key.split('__')
            return key, cls.tr_python[op], value
        return key, ope.eq, value

    @classmethod
    def translate_sql(cls, key, value):
        """ translate filter with (quasi)django notation to sql notation
        :param key: a key in (quasi)djano notation
        :param value: a value to compare
        :return: the equivalent sql expression
        """
        def quote(param):
            try:
                return int(param)
            except (ValueError, TypeError):
                return repr(param)
        if '__' in key:
            key, op = key.split('__')
            return key + cls.tr_sql[op] + quote(value)
        return key + '=' + quote(value)


class TableContainer:
    def __init__(self, table, fields, indexes=None):
        """ constructor
        :param table: (list) of fixed length tuples
        :param fields: (tuple) labels of columns in natural order
        :param indexes: (tuple) labels of columns that will be set as dict for fast search (each field must be unique)
        """
        self.table = table
        self.fields = fields
        self.idx = {k: i for i, k in enumerate(fields)}  # translates column name to column index
        self.indexes = indexes

    def __getitem__(self, item):
        return self.table[item]

    def __len__(self):
        return len(self.table)

    def __iter__(self):
        """ iterate over raw lines of table
        """
        return iter(self.table)

    def iter_tuple(self):
        """ iterate over lines of table rendered as tuples of pairs (label, value)
        """
        return (zip(self.fields, l) for l in self.table)

    def iter_dict(self):
        """ iterate over lines of table rendered as dict {label: value}
        """
        return (dict(zip(self.fields, l)) for l in self.table)

    def append(self, line):
        self.table.append(line)
        if self.indexes:
            # TODO update indexes
            pass

    def exclude(self, *filters):
        # TODO
        pass

    def filter(self, *filters):
        """ generator over filtered raw lines of the table
        :param filters: iterable of tuple (key, value) or (key, op, value)
               a AND operation is performed among them
        """
        # TODO make Q and F filters (filters should be objects)
        flt = []
        for f in filters:
            k, op, v = Filter.translate_py(*f)
            flt.append((self.idx[k], op, v))
        for l in self.table:
            skip = False
            for i, op, v in flt:
                if not op(l[i], v):
                    skip = True
                    break
            if skip:
                continue
            yield l

    def distinct(self, keys, filters=()):
        """ return distinct keys
        :param keys: (tuple)
        :param filters:
        """
        # TODO make a distinct method for single field instead of tuple of fields
        kidx = [self.idx[k] for k in keys]
        d = set()
        for l in self.filter(*filters):
            d.add(tuple(l[i] for i in kidx))
        return d

    def aggregate(self, keys, fields, filters=()):
        """ aggregate lines of table
        :param keys: (tuple) aggregate along this set of keys
        :param fields: list of pairs (key, aggregator) where aggregator is a dict-like class
        :param filters:
        """
        # TODO make an aggregate method for single field instead of tuple of fields
        kidx = [self.idx[k] for k in keys]
        fld = [(self.idx[k], v()) for k, v in fields]
        for l in self.filter(*filters):
            key = tuple(l[i] for i in kidx)
            for i, v in fld:
                v[key] = l[i]
        return [v for _, v in fld]


class CountSortMixin:
    def sorted(self, reverse=False):
        return sorted(((k, v) for k, v in self.items()), key=ope.itemgetter(1), reverse=reverse)


class LenSortMixin:
    def sorted(self, reverse=False):
        return sorted(((k, v) for k, v in self.items()), key=lambda x: len(x[1]), reverse=reverse)


class FilteredMixin:
    def __init__(self, op, value):
        dict.__init__(self)
        self.op = Filter.tr_python[op] if isinstance(op, basestring) else op
        self.value = value
    def __call__(self):
        return self


class Count(CountSortMixin, dict):
    def __setitem__(self, key, value):
        if key in self:
            dict.__setitem__(self, key, self[key] + 1)
        else:
            dict.__setitem__(self, key, 1)


class FilteredCount(FilteredMixin, CountSortMixin, dict):
    def __setitem__(self, key, value):
        if self.op(value, self.value):
            if key in self:
                dict.__setitem__(self, key, self[key] + 1)
            else:
                dict.__setitem__(self, key, 1)
        elif key not in self:
            dict.__setitem__(self, key, 0)


class Sum(CountSortMixin, dict):
    def __setitem__(self, key, value):
        if key in self:
            dict.__setitem__(self, key, self[key] + value)
        else:
            dict.__setitem__(self, key, value)


class AggList(LenSortMixin, dict):
    def __setitem__(self, key, value):
        if key in self:
            self[key].append(value)
        else:
            dict.__setitem__(self, key, [value])


class FilteredAggList(FilteredMixin, LenSortMixin, dict):
    def __setitem__(self, key, value):
        if self.op(value, self.value):
            if key in self:
                self[key].append(value)
            else:
                dict.__setitem__(self, key, [value])
        elif key not in self:
            dict.__setitem__(self, key, [])


class AggSet(LenSortMixin, dict):
    def __setitem__(self, key, value):
        if key in self:
            self[key].add(value)
        else:
            dict.__setitem__(self, key, {value})


class FilteredAggSet(FilteredMixin, LenSortMixin, dict):
    def __setitem__(self, key, value):
        if self.op(value, self.value):
            if key in self:
                self[key].add(value)
            else:
                dict.__setitem__(self, key, {value})
        elif key not in self:
            dict.__setitem__(self, key, set())
